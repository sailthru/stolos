# TODO this module
import redis
import redlock

from stolos import get_NS
from stolos import argparse_shared as at

from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue


def raw_client(self):
    NS = get_NS()
    return redis.StrictRedis(
        NS.qb_redis_host, NS.qb_redis_port, NS.qb_redis_db)


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        raise NotImplemented()

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        raise NotImplemented()

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
        raise NotImplemented()


class Lock(BaseLock):
    def __init__(self, path):
        ns = get_NS()
        redlock.Redlock(ns.qb_redis_host)
    def acquire(self, blocking=True, timeout=None):
        raise NotImplemented()

    def release(self):
        raise NotImplemented()

    def is_locked(self):
        """
        Return True if path is currently locked by anyone, and False otherwise
        """
        # TODO: this is really an exists.
        # redis calls exists. returns T|F)
        # return qbcli.Lock.exists(path)
        raise NotImplemented()


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
    raise NotImplementedError()


def create(path, value):
    """Set value at given path.
    If path already exists, raise stolos.exceptions.NodeExistsError
    """
    raise NotImplementedError()


build_arg_parser = at.build_arg_parser([
    at.add_argument('--qb_redis_host', default='127.0.0.1'),
    at.add_argument('--qb_redis_port', default='6379'),
    at.add_argument('--qb_redis_db', default='0'),
], description=(
    "These options specify which queue to use to store state about your jobs"))
