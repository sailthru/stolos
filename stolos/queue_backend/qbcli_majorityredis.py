from majorityredis import (MajorityRedis, retry_condition)
from majorityredis.exceptions import Timeout
import random
import time
import threading
import os
import signal
import redis
import sys

from stolos import log
from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util
import stolos.exceptions

from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue


@util.cached
def raw_client():
    NS = get_NS()
    clients = [
        redis.StrictRedis(
            host=host, port=port, socket_timeout=NS.qb_redis_socket_timeout)
        for host, port in NS.qb_redis_hosts]
    return clients[0]


@util.cached
def raw_client_mr():
    NS = get_NS()
    return MajorityRedis(
        [raw_client()], NS.qb_redis_n_servers or len(NS.qb_redis_hosts),
        getset_history_prefix=NS.qb_redis_history_prefix, threadsafe=True)


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        self._q = raw_client_mr().LockingQueue(path)
        self._item = None
        self._h_k = None

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        self._q.put(value, priority, retry_condition(nretry=10))

    def consume(self):
        """Consume value gotten from queue.
        Raise UserWarning if consume() called before get()
        """
        if self._item is None:
            raise UserWarning("Must call get() before consume()")
        self._q.consume(self._h_k)
        self._h_k = None
        self._item = None

    def get(self, timeout=None):
        """Get an item from the queue or return None.  Do not block forever."""
        if self._item is not None:
            return self._item
        if timeout:
            gett = retry_condition(
                nretry=int(timeout) + 2, backoff=lambda x: 1,
                condition=lambda rv: rv is not None, timeout=timeout
            )(self._q.get)
        else:
            gett = self._q.get
        try:
            rv = gett()
        except Timeout:
            return
        if rv is None:
            return
        self._item, self._h_k = i, h_k = rv
        return i

    def size(self, queued=True, taken=True):
        """
        Find the number of jobs in the queue

        `queued` - Include the entries in the queue that are not currently
            being processed or otherwise locked
        `taken` - Include the entries in the queue that are currently being
            processed or are otherwise locked

        Raise AttributeError if all kwargs are False
        """
        if not queued and not taken:
            raise AttributeError("given kwargs cannot all be False")
        return self._q.size(queued=queued, taken=taken)

    def is_queued(self, value):
        """
        Return True if item is in queue or currently being processed.
        False otherwise

        Redis will not like this operation.  Use sparingly with large queues.
        """
        return self._q.is_queued(item=value)


def _raise_err(x, y):
    time.sleep(1)  # hack to better deal with cleanup
    raise Exception("Received an exit signal from queue backend thread"
                    " and now exiting")


class Lock(BaseLock):
    LOCKS = dict()  # path: client_id
    SHAS = {}
    SCRIPTS = dict(
        # returns 1 if locked, 0 if could not lock.
        # return exception if invalid expireat (ie lock is already expired)
        l_lock=dict(keys=('path', ), args=('client_id', 'expireat'), script="""
    if 1 == redis.call("SETNX", KEYS[1], ARGV[1]) then
        if 1 == redis.call("EXPIREAT", KEYS[1], ARGV[2]) then return 1
        else
        redis.call("DEL", KEYS[1])
        return {err="invalid expireat"} end
    elseif ARGV[1] == redis.call("GET", KEYS[1]) then
        if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[2]) then
        redis.call("DEL", KEYS[1])
        return {err="invalid expireat"} end
        return 1
    else return 0 end
    """),

        # returns 1 if unlocked, 0 otherwise
        l_unlock=dict(keys=('path', ), args=('client_id', ), script="""
    local rv = redis.call("GET", KEYS[1])
    if rv == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    elseif rv == false then return 1
    else return 0 end
    """),

        # returns 1 if got lock extended, 0 otherwise
        l_extend_lock=dict(
            keys=('path', ), args=('client_id', 'expireat'), script="""
    if ARGV[1] == redis.call("GET", KEYS[1]) then
        return redis.call("EXPIREAT", KEYS[1], ARGV[2])
    else return {err="don't own lock"} end
    """),
    )
    _INITIALIZED = False

    def __init__(self, path):
        self._client_id = str(random.randint(0, sys.maxint))
        self._path = path

        self._lock_timeout = get_NS().qb_redis_lock_timeout
        self._max_network_delay = get_NS().qb_redis_max_network_delay
        if not Lock._INITIALIZED:
            Lock._INITIALIZED = True
            # don't replace any signal handlers if already set
            for sig in [signal.SIGUSR1, signal.SIGUSR2,
                        'fail-no signals availble']:
                if signal.getsignal(sig) == 0:
                    Lock._SIGNAL = sig
                    break
            signal.signal(Lock._SIGNAL, _raise_err)
            # submit Lock lua scripts to redis
            for k in Lock.SCRIPTS:
                Lock.SHAS[k] = \
                    raw_client().script_load(Lock.SCRIPTS[k]['script'])
            # start extending locks in the background
            t = threading.Thread(
                name="queue_backend.majorityredis.Lock Extender",
                target=self._extend_lock_in_background)
            t.daemon = True
            t.start()

    def _extend(self, path, client_id, results):
        extended = raw_client().evalsha(
            Lock.SHAS['l_extend_lock'],
            len(Lock.SCRIPTS['l_extend_lock']['keys']), path,
            client_id, int(time.time() + self._lock_timeout) + 1)
        results.append((path, client_id, extended))

    def _extend_lock_in_background(self):
        """
        background signal (not a thread) that keeps lock alive
        """
        while True:
            try:
                s = time.time()
                threads = []
                results = []
                for path in list(Lock.LOCKS):
                    try:
                        client_id = Lock.LOCKS[path]
                    except KeyError:
                        continue  # race condition optimization
                    t = threading.Thread(
                        name="Lock Extend %s %s" % (path, client_id),
                        target=self._extend, args=(path, client_id, results))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
                for x in results:
                    if x[2] != 1:
                        raise Exception(
                            "Failed to extend lock for path: %s. redis_msg: %s"
                            % (x[0], x[2]))

                # adjust sleep time based on min expireat
                delta = time.time() - s
                sleep_time = \
                    self._lock_timeout - self._max_network_delay - delta
                assert self._lock_timeout > sleep_time > 0, (
                    'took too long to extend the locks.'
                    ' increase lock_timeout or reduce number of concurrent'
                    ' locks you have or improve network latency')
                time.sleep(sleep_time)
            except Exception as err:
                # kill parent process
                log.error(
                    'Redis Queue Backend asking Stolos to exit(1)'
                    ' because of err: %s' % err)
                os.kill(os.getpid(), Lock._SIGNAL)
                raise

    def _acquire(self, path, client_id, nx=False, xx=False):
        expireat = int(time.time() + self._lock_timeout)
        return 1 == raw_client().evalsha(
            Lock.SHAS['l_lock'],
            len(Lock.SCRIPTS['l_lock']['keys']),
            self._path, self._client_id, expireat)

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        acquired = self._acquire(self._path, self._client_id, nx=True)

        if not acquired and blocking:
            c = 0
            while True:
                c += 1
                time.sleep(1)
                if c >= timeout:
                    break
                acquired = self._acquire(self._path, self._client_id, nx=True)

        if acquired:
            Lock.LOCKS[self._path] = self._client_id
        return acquired

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  Raise UserWarning otherwise, possibly due to:
            - did not release a lock
            - lock already released
            - lock does not exist (perhaps it was never acquired)
        """
        if Lock.LOCKS.get(self._path) != self._client_id:
            raise UserWarning("You must acquire lock before releasing it")
        Lock.LOCKS.pop(self._path)

        try:
            rv = raw_client().evalsha(
                Lock.SHAS['l_unlock'],
                len(Lock.SCRIPTS['l_unlock']['keys']),
                self._path, self._client_id)
            assert rv == 1  # TODO
        except AssertionError:
            raise UserWarning("Lock did not exist on Redis server")
        except Exception as err:
            msg = "Could not release lock.  Got error: %s" % err
            log.exception(msg, extra=dict(lock_path=self._path))
            raise UserWarning(msg)

    def is_locked(self):
        """
        Return True if path is currently locked by anyone, and False otherwise
        """
        return raw_client().exists(self._path)


def get(path):
    """Get value at given path.
    If path does not exist, throw stolos.exceptions.NoNodeError
    """
    rv = raw_client().get(path)
    if rv is None:
        raise stolos.exceptions.NoNodeError
    if rv == '--STOLOSEMPTYSTRING--':
        rv = ''
    return rv


def exists(path):
    """Return True if path exists (value can be ''), False otherwise"""
    return raw_client().exists(path)


def delete(path, _recursive=False):
    """Remove path from queue backend.

    `_recursive` - This is only for tests
    """
    mr = raw_client()
    if _recursive:
        # For tests only
        keys = mr.keys('*%s*' % path)
        if not keys:
            return True
        return mr.delete(*keys) == len(keys)
    else:
        return mr.delete(path)


def set(path, value):
    """Set value at given path
    If the path does not already exist, raise stolos.exceptions.NoNodeError
    """
    if value == '':
        value = '--STOLOSEMPTYSTRING--'
    rv = raw_client().set(path, value, xx=True)
    if not rv:
        raise stolos.exceptions.NoNodeError("Could not set path: %s" % path)


def create(path, value):
    """Set value at given path.
    If path already exists, raise stolos.exceptions.NodeExistsError
    """
    if value == '':
        value = '--STOLOSEMPTYSTRING--'
    rv = raw_client().set(path, value, nx=True)
    if not rv:
        raise stolos.exceptions.NodeExistsError(
            "Could not create path: %s" % path)


def increment(path, value=1):
    """Increment the counter at given path
    Return the incremented count as an int
    """
    rc = raw_client()
    return rc.incrby(path, value)


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--qb_redis_hosts', required=True,
        type=lambda x: [y.split(':') for y in x.split(',')],
        help="Redis servers to connect to in form: host1:port1,host2:port2"
    ),
    at.add_argument('--qb_redis_db', default=0, type=int),
    at.add_argument('--qb_redis_lock_timeout', default=60, type=int),
    at.add_argument('--qb_redis_max_network_delay', default=30, type=int),
    at.add_argument(
        '--qb_redis_socket_timeout', default='5', type=float, help=(
            "number of seconds that the redis client will spend waiting for a"
            " response from Redis.")),
    at.add_argument(
        '--qb_redis_n_servers', type=int, help=(
            "The total number of Redis servers that Stolos may connect to."
            " This number is constant over time.  If you increase it, you may"
            " have data inconsistency issues."
            " By default, infer this from number of defined hosts")),
    at.add_argument(
        '--qb_redis_history_prefix', default='stolos', help=(
            "majorityredis stores the history of read and write operations"
            " in a key prefixed by the given value. You probably don't need to"
            " modify the default value.")),
], description=(
    "These options specify which queue to use to store state about your jobs"))
