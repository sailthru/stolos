from majorityredis import (
    MajorityRedis, retry_condition)
import redis

from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util
import stolos.exceptions

from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue


@util.cached
def raw_client(self):
    NS = get_NS()
    clients = [
        redis.StrictRedis(
            host=host, port=port, socket_timeout=NS.qb_redis_socket_timeout)
        for host, port in NS.qb_redis_hosts]
    return MajorityRedis(
        clients, NS.qb_redis_n_servers or len(NS.qb_redis_hosts))


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        self._q = raw_client().LockingQueue(path)
        self._h_k = None

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        self._q.put(value, priority, retry_condition(10))

    def consume(self):
        """Consume value gotten from queue.
        Raise UserWarning if consume() called before get()
        """
        if not self._h_k:
            raise UserWarning("Must call get() before consume()")
        self._q.consume(self._h_k)
        self._h_k = None

    def get(self, timeout=None):
        """Get an item from the queue or return None"""
        i, h_k = self._q.get()
        self._h_k = h_k
        return i

    def size(self, queued=True, taken=True):
        """
        Find the number of jobs in the queue

        `queued` - Include the entries in the queue that are not currently
            being processed or otherwise locked
        `taken` - Include the entries in the queue that are currently being
            processed or are otherwise locked

        Raise AttributeError if both queued=False and taken=False
        """
        if not queued and not taken:
            raise AttributeError("queued and taken cannot both be False")
        return self._q.size(queued=queued, taken=taken)

    def is_queued(self, value):
        """
        Return True if item is in queue or currently being processed.
        False otherwise
        """
        return self._q.is_queued(item=value)


class Lock(BaseLock):
    def __init__(self, path):
        self._path = path

    def acquire(self, blocking=True, timeout=None):
        """
        Acquire a lock at the Lock's path.

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        return raw_client().Lock.lock(wait_for=timeout)

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  False if:
            - did not release a lock
            - if lock already released
            - if lock does not exist
        """
        return 50 < raw_client().Lock.unlock(self._path)

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
        fail = False
        for k in set(y for y in (x.keys('%s*' % path) for x in mr._clients)):
            fail |= not mr.delete(k)
        return not fail
    else:
        return mr.delete(path)


def set(path, value):
    """Set value at given path
    If the path does not already exist, raise stolos.exceptions.NoNodeError
    """
    rv = raw_client().set(path, value, retry_condition(10), xx=True)
    if not rv:
        raise stolos.exceptions.NoNodeError("Could not set path: %s" % path)


def create(path, value):
    """Set value at given path.
    If path already exists, raise stolos.exceptions.NodeExistsError
    """
    rv = raw_client().set(path, value, retry_condition(10), nx=True)
    if not rv:
        raise stolos.exceptions.NoNodeError("Could not create path: %s" % path)


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--qb_redis_hosts', default=[('127.0.0.1', 6379)],
        type=lambda x: [y.split(':') for y in x.split(',')],
        help="Redis servers to connect to in form: host1:port1,host2:port2"
    ),
    at.add_argument('--qb_redis_qb_socket_timeout', default='5', help=(
        "number of seconds that the redis client will spend waiting for a"
        " response from Redis.")),
    at.add_argument(
        '--qb_redis_n_servers', required=True, type=int, help=(
            "The total number of Redis servers that Stolos may connect to."
            " This number is constant over time.  If you increase it, you may"
            " have data inconsistency issues")),
], description=(
    "These options specify which queue to use to store state about your jobs"))
