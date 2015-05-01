# TODO this module
from majorityredis import MajorityRedis, exceptions as mrexceptions
import redis
import time

from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util

from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue
from . import log


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

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        pct, h_k = self._q.put(value, priority=priority)
        n = 0
        while pct < 50:
            pct, h_k = self._q.put(h_k, is_hash=True)
            time.sleep(1)
            if n % 10 == 0:
                log.warn(
                    "Failed to put item onto LockingQueue.  Retrying...",
                    extra=dict(item=value, item_hash=h_k))
            n += 1
            if n > 30:
                raise RuntimeError(
                    "The Redis queue backend must be unstable because I cannot"
                    "put to the majority of servers")
        # store most recent value hash
        set(value, h_k)  # TODO: or create
        return
        # TODO: return value?

    def consume(self):
        """Consume value gotten from queue.
        Raise UserWarning if consume() called before get()
        """
        raise NotImplemented()

    def get(self, timeout=None):
        """Get an item from the queue or return None"""
        raise NotImplemented()

    def size(self, queued=True, taken=True):
        """
        Find the number of jobs in the queue

        `queued` - Include the entries in the queue that are not currently
            being processed or otherwise locked
        `taken` - Include the entries in the queue that are currently being
            processed or are otherwise locked

        Raise AttributeError if both queued=False and taken=False
        """
        raise NotImplemented()

    def is_queued(self, value):
        """
        Return True if item is in queue or currently being processed.
        False otherwise
        """
        cli = raw_client()
        h_k = cli.get(value)
        # TODO: if zscore cli, it exists
        raise NotImplemented()


class Lock(BaseLock):
    def __init__(self, path):
        self._path = path

    def acquire(self, blocking=True, timeout=None):
        return raw_client.Lock.lock(self._path)

    def release(self):
        return raw_client.Lock.unlock(self._path)

    def is_locked(self):
        """
        Return True if path is currently locked by anyone, and False otherwise
        """
        return raw_client().exists(self._path)


def delete(path, recursive=False):
    """Remove path from queue backend"""
    raise NotImplementedError()


def get(path):
    """Get value at given path.
    If path does not exist, throw stolos.exceptions.NoNodeError
    """
    raise NotImplementedError()


def get_children(path):
    """Get names of child nodes under given path
    If path does not exist, throw stolos.exceptions.NoNodeError
    """
    raise NotImplementedError()


def count_children(path):
    """Count number of child nodes at given parent path
    If the path does not already exist, raise stolos.exceptions.NoNodeError
    """
    raise NotImplementedError()


def exists(path):
    """Return True if path exists (value can be ''), False otherwise"""
    raise NotImplementedError()


def set(path, value):
    """Set value at given path
    If the path does not already exist, raise stolos.exceptions.NoNodeError
    """
    n = 0
    rv = False
    while True:
        try:
            rv = raw_client().set(value, h_k, nx=True)  # TODO from here
        except mrexceptions.NoMajority:
            pass
        if rv:
            break
        n += 1
        time.sleep(1)
        if n > 10:
            raise RuntimeError(
                "The Redis queue backend must be unstable because I cannot"
                "set on the majority of servers")
    raise NotImplementedError()


def create(path, value):
    """Set value at given path.
    If path already exists, raise stolos.exceptions.NodeExistsError
    """
    raise NotImplementedError()


build_arg_parser = at.build_arg_parser([
    at.add_argument('--qb_redis_hosts', default=[('127.0.0.1', 6379)]),
    at.add_argument('--qb_redis_qb_socket_timeout', default='3'),
    at.add_argument('--qb_redis_n_servers', default=None),
], description=(
    "These options specify which queue to use to store state about your jobs"))
