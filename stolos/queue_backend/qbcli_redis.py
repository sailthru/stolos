from contextlib import contextmanager
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


@contextmanager
def timeout_cm(seconds):
    if not seconds:
        yield
    else:
        orig = signal.getsignal(signal.SIGALRM)
        # replace the alarm handler only if it isn't already set
        # this could be dangerous if other code concurrently uses SIGALRM
        signal.signal(signal.SIGALRM, lambda x, y: 'do nothing')
        signal.alarm(seconds)
        try:
            yield
            signal.alarm(0)
        finally:
            signal.signal(signal.SIGALRM, orig)


@util.cached
def raw_client():
    NS = get_NS()
    return redis.StrictRedis(
        host=NS.qb_redis_host,
        port=NS.qb_redis_port,
        socket_timeout=NS.qb_redis_socket_timeout)


class BaseStolosRedis(object):
    """
    Base Class for Lock and LockingQueue that initializes a few things
    - set LOCKS and _SHAS as class variables on each child class's first
      initialization
    - provide tooling that automatically extends locks in the background using
    the script provided by _EXTEND_LOCK_SCRIPT_NAME
    """
    _INITIALIZED = False
    _BASE_INITIALIZED = False

    SCRIPTS = dict()  # filled out by child classes

    def __init__(self, path):
        assert self.SCRIPTS, 'child class must define SCRIPTS'
        assert self._EXTEND_LOCK_SCRIPT_NAME, (
            'child class must define _EXTEND_LOCK_SCRIPT_NAME')

        self._client_id = str(random.randint(0, sys.maxsize))
        self._path = path

        self._lock_timeout = get_NS().qb_redis_lock_timeout
        self._max_network_delay = get_NS().qb_redis_max_network_delay

        if not BaseStolosRedis._BASE_INITIALIZED:
            BaseStolosRedis._BASE_INITIALIZED = True

            # use signals to trigger main thread to exit if child thread errors
            # be nice and don't replace any signal handlers if already set
            for sig in [signal.SIGUSR1, signal.SIGUSR2,
                        'fail: no user-level signals available']:
                if signal.getsignal(sig) == 0:
                    BaseStolosRedis._SIGNAL = sig
                    break
            signal.signal(BaseStolosRedis._SIGNAL, _raise_err)

        if not self._INITIALIZED:
            self._INITIALIZED = True
            self.LOCKS = dict()

            # submit class's lua scripts to redis and store the SHA's
            self._SHAS = dict()
            for k in self.SCRIPTS:
                self._SHAS[k] = raw_client().script_load(
                    self.SCRIPTS[k]['script'])

            # initialize a lock extender thread for each class type that exists
            # we could just group all together, but this seems like a good idea
            # start extending locks in the background
            t = threading.Thread(
                name=("stolos.queue_backend.qbcli_redis.%s Extender"
                      % self.__class__.__name__),
                target=self._extend_lock_in_background)
            t.daemon = True
            t.start()

    def _extend_lock_in_background(self):
        """
        background signal (not a thread) that keeps lock alive
        """
        while True:
            try:
                s = time.time()
                threads = []
                results = []
                for path in list(self.LOCKS):
                    try:
                        client_id = self.LOCKS[path]
                    except KeyError:
                        continue  # race condition optimization
                    t = threading.Thread(
                        name="%s Extend %s %s" % (
                            self.__class__.__name__, path, client_id),
                        target=self._extend, args=(path, client_id, results))
                    t.daemon = True
                    t.start()
                    threads.append(t)
                for t in threads:
                    t.join()
                if len(threads) != len(results):
                    raise Exception("Failed to extend lock")
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
                os.kill(os.getpid(), BaseStolosRedis._SIGNAL)
                raise

    def _extend(self, path, client_id, results):
        script = self._EXTEND_LOCK_SCRIPT_NAME
        extended = raw_client().evalsha(
            self._SHAS[script],
            len(self.SCRIPTS[script]['keys']), path,
            client_id, int(time.time() + self._lock_timeout) + 1)
        results.append((path, client_id, extended))


class LockingQueue(BaseStolosRedis, BaseLockingQueue):

    _EXTEND_LOCK_SCRIPT_NAME = 'lq_extend_lock'
    # Lua scripts that are sent to redis
    # keys:
    # h_k = ordered hash of key in form:  priority:insert_time_since_epoch:key
    # Q = sorted set of queued keys, h_k
    # Qi = sorted mapping (h_k -> key) for all known queued or completed items
    #
    # args:
    # expireat = seconds_since_epoch, presumably in the future
    # client_id = unique owner of the lock
    # randint = a random integer that changes every time script is called
    SCRIPTS = dict(

        # returns 1
        lq_put=dict(keys=('Q', 'h_k'), args=(), script="""
redis.call("ZINCRBY", KEYS[1], 0, KEYS[2])
return 1
"""),

        # returns 1 if got an item, and returns an error otherwise
        lq_get=dict(keys=('Q', ), args=('client_id', 'expireat'), script="""
local h_k = redis.call("ZRANGE", KEYS[1], 0, 0)[1]
if nil == h_k then return {err="queue empty"} end
if false == redis.call("SET", h_k, ARGV[1], "NX") then
return {err="already locked"} end
if 1 ~= redis.call("EXPIREAT", h_k, ARGV[2]) then
return {err="invalid expireat"} end
redis.call("ZINCRBY", KEYS[1], 1, h_k)
return h_k
"""),

        # returns 1 if got lock. Returns an error otherwise
        lq_lock=dict(
            keys=('h_k', 'Q'), args=('expireat', 'randint', 'client_id'),
            script="""
if false == redis.call("SET", KEYS[1], ARGV[3], "NX") then  -- did not get lock
local rv = redis.call("GET", KEYS[1])
if rv == "completed" then
    redis.call("ZREM", KEYS[2], KEYS[1])
    return {err="already completed"}
elseif rv == ARGV[3] then
    if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[1]) then
    return {err="invalid expireat"} end
    return 1
else
    local score = tonumber(redis.call("ZSCORE", KEYS[2], KEYS[1]))
    math.randomseed(tonumber(ARGV[2]))
    local num = math.random(math.floor(score) + 1)
    if num ~= 1 then
    redis.call("ZINCRBY", KEYS[2], (num-1)/score, KEYS[1])
    end
    return {err="already locked"}
end
else
if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[1]) then
    return {err="invalid expireat"} end
redis.call("ZINCRBY", KEYS[2], 1, KEYS[1])
return 1
end
"""),

        # return 1 if extended lock.  Returns an error otherwise.
        # otherwise
        lq_extend_lock=dict(
            keys=('h_k', ), args=('client_id', 'expireat'), script="""
local rv = redis.call("GET", KEYS[1])
if ARGV[1] == rv then
    if 1 ~= redis.call("EXPIREAT", KEYS[1], ARGV[2]) then
    return {err="invalid expireat"} end
    return 1
elseif "completed" == rv then return {err="already completed"}
elseif false == rv then return {err="expired"}
else return {err="lock stolen"} end
"""),

        # returns 1 if removed, 0 if key was already removed.
        lq_consume=dict(
            keys=('h_k', 'Q', 'Qi'), args=('client_id', ), script="""
local rv = redis.pcall("GET", KEYS[1])
if ARGV[1] == rv or "completed" == rv then
redis.call("SET", KEYS[1], "completed")
redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
redis.call("ZREM", KEYS[2], KEYS[1])
if "completed" ~= rv then redis.call("INCR", KEYS[3]) end
return 1
else return 0 end
"""),

        # returns nil.  markes job completed
        lq_completed=dict(
            keys=('h_k', 'Q', 'Qi'), args=(), script="""
if "completed" ~= redis.call("GET", KEYS[1]) then
redis.call("INCR", KEYS[3])
redis.call("SET", KEYS[1], "completed")
redis.call("PERSIST", KEYS[1])  -- or EXPIRE far into the future...
redis.call("ZREM", KEYS[2], KEYS[1])
end
"""),

        # returns 1 if removed, 0 otherwise
        lq_unlock=dict(
            keys=('h_k', ), args=('client_id', ), script="""
if ARGV[1] == redis.call("GET", KEYS[1]) then
    return redis.call("DEL", KEYS[1])
else return 0 end
"""),

        # returns number of items {(queued + taken), completed}
        # O(log(n))
        lq_qsize_fast=dict(
            keys=('Q', 'Qi'), args=(), script="""
return {redis.call("ZCARD", KEYS[1]), redis.call("INCRBY", KEYS[2], 0)}"""),

        # returns number of items {in_queue, taken, completed}
        # O(n)  -- eek!
        lq_qsize_slow=dict(
            keys=('Q', 'Qi'), args=(), script="""
local taken = 0
local queued = 0
for _,k in ipairs(redis.call("ZRANGE", KEYS[1], 0, -1)) do
local v = redis.call("GET", k)
if "completed" ~= v then
    if v then taken = taken + 1
    else queued = queued + 1 end
end
end
return {queued, taken, redis.call("INCRBY", KEYS[2], 0)}
"""),

        # returns whether an item is in queue or currently being processed.
        # returns boolean tuple of form:  (is_taken, is_queued, is_completed)
        # O(1)
        lq_is_queued_h_k=dict(
            keys=('Q', 'h_k'), args=(), script="""
local taken = redis.call("GET", KEYS[2])
if "completed" == taken then
return {false, false, true}
elseif taken then return {true, false, false}
else return {false, false ~= redis.call("ZSCORE", KEYS[1], KEYS[2]), false} end
"""),

        # returns whether an item is in queue or currently being processed.
        # raises an error if already completed.
        # O(N * strlen(item)) -- eek!
        lq_is_queued_item=dict(
            keys=('Q', 'item'), args=(), script="""
for _,k in ipairs(redis.call("ZRANGE", KEYS[1], 0, -1)) do
if string.sub(k, -string.len(KEYS[2])) == KEYS[2] then
    local taken = redis.call("GET", k)
    if taken then
    if "completed" == taken then return {false, false, true} end
    return {true, false, false}
    else
    return {false, true, false} end
end
end
return {false, false, false}
"""),
    )

    def __init__(self, path):
        super(LockingQueue, self).__init__(path)
        self._q_lookup = ".%s" % path

        self._item = None
        self._h_k = None

    def __del__(self):
        for k in list(self.LOCKS):
            if self.LOCKS.get(k) == self._client_id:
                self.LOCKS.pop(k)

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        # format into hashed key
        h_k = "%d:%f:%s" % (priority, time.time(), value)

        rv = raw_client().evalsha(
            self._SHAS['lq_put'],
            len(self.SCRIPTS['lq_put']['keys']),
            self._path, h_k)
        assert rv == 1

    def consume(self):
        """Consume value gotten from queue.
        Raise UserWarning if consume() called before get()
        """
        if self._item is None:
            raise UserWarning("Must call get() before consume()")

        self.LOCKS.pop(self._h_k)

        rv = raw_client().evalsha(
            self._SHAS['lq_consume'],
            len(self.SCRIPTS['lq_consume']['keys']),
            self._h_k, self._path, self._q_lookup, self._client_id)
        assert rv == 1

        self._h_k = None
        self._item = None

    def get(self, timeout=None):
        """Get an item from the queue or return None.  Do not block forever."""
        if self._item is not None:
            return self._item

        expire_at = int(time.time() + self._lock_timeout)

        with timeout_cm(timeout):  # won't block forever
            try:
                self._h_k = raw_client().evalsha(
                    self._SHAS['lq_get'],
                    len(self.SCRIPTS['lq_get']['keys']),
                    self._path, self._client_id, expire_at)
            except redis.exceptions.ResponseError as err:
                if str(err) not in ['queue empty', 'already locked']:
                    raise err

        if self._h_k:
            priority, insert_time, item = self._h_k.decode().split(':', 2)
            self._item = item
            self.LOCKS[self._h_k] = self._client_id
            return self._item

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
            raise AttributeError("either `taken` or `queued` must be True")

        if taken and queued:

            n_queued_and_taken, _ = raw_client().evalsha(
                self._SHAS['lq_qsize_fast'],
                len(self.SCRIPTS['lq_qsize_fast']['keys']),
                self._path, self._q_lookup)
            return n_queued_and_taken
        else:
            nqueued, ntaken, _ = raw_client().evalsha(
                self._SHAS['lq_qsize_slow'],
                len(self.SCRIPTS['lq_qsize_slow']['keys']),
                self._path, self._q_lookup)
            if queued:
                return nqueued
            elif taken:
                return ntaken
            else:
                raise Exception('code error - should never get here')

    def is_queued(self, value):
        """
        Return True if item is in queue or currently being processed.
        False otherwise

        Redis will not like this operation.  Use sparingly with large queues.
        """
        if value == self._item:
            taken, queued, completed = raw_client().evalsha(
                self._SHAS['lq_is_queued_h_k'],
                len(self.SCRIPTS['lq_is_queued_h_k']['keys']),
                self._path, self._h_k)
        else:
            taken, queued, completed = raw_client().evalsha(
                self._SHAS['lq_is_queued_item'],
                len(self.SCRIPTS['lq_is_queued_item']['keys']),
                self._path, value)
        return taken or queued


def _raise_err(x, y):
    time.sleep(1)  # hack to better deal with cleanup
    raise Exception("Received an exit signal from queue backend thread"
                    " and now exiting")


class Lock(BaseStolosRedis, BaseLock):
    _EXTEND_LOCK_SCRIPT_NAME = 'l_extend_lock'
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

    def _acquire(self, path, client_id, nx=False, xx=False):
        expireat = int(time.time() + self._lock_timeout)
        return 1 == raw_client().evalsha(
            self._SHAS['l_lock'],
            len(self.SCRIPTS['l_lock']['keys']),
            self._path, self._client_id, expireat)

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        if not blocking:
            timeout = None
        with timeout_cm(timeout):
            acquired = self._acquire(self._path, self._client_id, nx=True)

        if acquired:
            self.LOCKS[self._path] = self._client_id
        return acquired

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  Raise UserWarning otherwise, possibly due to:
            - did not release a lock
            - lock already released
            - lock does not exist (perhaps it was never acquired)
        """
        if self.LOCKS.get(self._path) != self._client_id:
            raise UserWarning("You must acquire lock before releasing it")
        self.LOCKS.pop(self._path)

        try:
            rv = raw_client().evalsha(
                self._SHAS['l_unlock'],
                len(self.SCRIPTS['l_unlock']['keys']),
                self._path, self._client_id)
            assert rv == 1
        except AssertionError:
            raise UserWarning("Lock did not exist on Redis server")
        except Exception as err:
            msg = "Could not release lock.  Got error: %s" % err
            log.error(msg, extra=dict(lock_path=self._path))
            raise UserWarning(msg)

    def is_locked(self):
        """
        Return True if path is currently locked by anyone, and False otherwise
        """
        return bool(raw_client().exists(self._path))


def get(path):
    """Get value at given path.
    If path does not exist, throw stolos.exceptions.NoNodeError
    """
    rv = raw_client().get(path)
    if rv is None:
        raise stolos.exceptions.NoNodeError(path)
    rv = rv.decode()
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
    at.add_argument('--qb_redis_host', help="Host address to redis server"),
    at.add_argument(
        '--qb_redis_port', type=int, default=6379,
        help="Port to connect to Redis server"),
    at.add_argument('--qb_redis_db', default=0, type=int),
    at.add_argument('--qb_redis_lock_timeout', default=60, type=int),
    at.add_argument('--qb_redis_max_network_delay', default=30, type=int),
    at.add_argument(
        '--qb_redis_socket_timeout', default='15', type=float, help=(
            "number of seconds that the redis client will spend waiting for a"
            " response from Redis.")),
], description=(
    "These options specify which queue to use to store state about your jobs"))
