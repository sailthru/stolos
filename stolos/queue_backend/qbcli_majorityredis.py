import __builtin__
from majorityredis import (MajorityRedis, retry_condition)
from majorityredis.exceptions import Timeout
import redis
import sys

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
    return MajorityRedis(
        clients, NS.qb_redis_n_servers or len(NS.qb_redis_hosts),
        getset_history_prefix=NS.qb_redis_history_prefix, threadsafe=True)


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        self._q = raw_client().LockingQueue(path)
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


class Lock(BaseLock):
    def __init__(self, path):
        self._path = path
        self._l = raw_client().Lock()

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        if blocking and timeout is None:
            timeout = sys.maxsize
        return bool(self._l.lock(self._path, wait_for=timeout))

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  Raise UserWarning otherwise, possibly due to:
            - did not release a lock
            - lock already released
            - lock does not exist (perhaps it was never acquired)
        """
        if 50 > self._l.unlock(self._path):
            raise UserWarning("Did not release lock")

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
        success = True
        for k in __builtin__.set(
                y for x in mr._clients for y in x.keys('*%s*' % path)):
            if (("%s/" % path) in k) or k.endswith(path):
                success &= mr.delete(k)
        return success
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
        '--qb_redis_n_servers', required=True, type=int, help=(
            "The total number of Redis servers that Stolos may connect to."
            " This number is constant over time.  If you increase it, you may"
            " have data inconsistency issues")),
    at.add_argument(
        '--qb_redis_history_prefix', default='stolos', help=(
            "majorityredis stores the history of read and write operations"
            " in a key prefixed by the given value. You probably don't need to"
            " modify the default value.")),
], description=(
    "These options specify which queue to use to store state about your jobs"))
