from majorityredis import (MajorityRedis, retry_condition)
from majorityredis.exceptions import Timeout
import redis
import threading
import random
import os
import sys
import signal
import time

from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util
from stolos import log
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


LOCKS = dict()  # path: client_id


class Lock(BaseLock):
    # a set of (lock_path, lock_timeout) pairs
    _INITIALIZED = False

    min_lock_refresh_interval = 1

    def __init__(self, path):
        self._path = path

        # code to deal with extending locks in background
        self._lock_timeout = 3  # seconds
        self._max_network_delay = 2  # seconds
        self._client_id = random.randint(0, sys.maxint)
        if not Lock._INITIALIZED:
            Lock._INITIALIZED = True
            self._extend_lock_in_background()

    def _extend_lock_in_background(self):
        """
        background signal (not a thread) that keeps lock alive
        """
        try:
            s = time.time()
            for path in list(LOCKS):
                if path not in LOCKS:
                    continue  # race condition optimization
                print(path, raw_client().exists(path))
                if not self._acquire(client_id=LOCKS[path], xx=True) and \
                        path in LOCKS:
                    raise Exception(
                        "Failed to extend lock for path: %s" % path)

            delta = time.time() - s
            sleep_time = self._lock_timeout - self._max_network_delay - delta
            assert self._lock_timeout > sleep_time > 0, sleep_time
            threading.Timer(
                sleep_time, self._extend_lock_in_background).start()
        except Exception as err:
            # kill parent process
            log.exception(err)
            log.error('Redis Queue Backend asking to exit(1)')
            os.kill(os.getpid(), signal.SIGINT)
            raise

    def _acquire(self, client_id, nx=False, xx=False):
        return bool(raw_client().set(
            self._path, client_id, nx=nx, xx=xx, ex=self._lock_timeout))

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        acquired = self._acquire(self._client_id, nx=True)

        if not acquired and blocking:
            c = 0
            while True:
                c += 1
                time.sleep(1)
                if c >= timeout:
                    break
                acquired = bool(raw_client().set(
                    self._path, nx=True, ex=self._lock_timeout))

        if acquired:
            LOCKS[self._path] = self._client_id
        return acquired

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  Raise UserWarning otherwise, possibly due to:
            - did not release a lock
            - lock already released
            - lock does not exist (perhaps it was never acquired)
        """
        try:
            LOCKS.pop(self._path)
        except KeyError:
            raise UserWarning("You must acquire lock before releasing it")
        # could there be a chance of a race condition here?
        # not sure how itimer signals work at kernel level...
        try:
            assert raw_client().delete(self._path)
        except AssertionError:
            raise UserWarning("Lock did not exist on Redis server")
        except:
            msg = "Could not release lock.  Dunno why"
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
    rv = raw_client().set(path, value, xx=True)
    if not rv:
        raise stolos.exceptions.NoNodeError("Could not set path: %s" % path)


def create(path, value):
    """Set value at given path.
    If path already exists, raise stolos.exceptions.NodeExistsError
    """
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
