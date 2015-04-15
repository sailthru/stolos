import atexit
from kazoo.client import (
    KazooClient,
    NoNodeError,
    NodeExistsError,
    Lock as _zkLock, LockingQueue as _zkLockingQueue
)
from os.path import join

from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util
from stolos import exceptions
from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue
from . import log


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        self._path = path
        self._q = _zkLockingQueue(client=raw_client(), path=path)

    def put(self, value, priority=100):
        self._q.put(value, priority=priority)

    def consume(self):
        if self._q.consume() is None:
            raise UserWarning(
                "Cannot consume() from queue without first calling q.get()")

    def get(self, timeout=None):
        return self._q.get(timeout=timeout)

    def size(self, queued=True, taken=True):
        """
        Find the number of jobs in the queue

        `queued` - Include the entries in the queue that are not currently
            being processed or otherwise locked
        `taken` - Include the entries in the queue that are currently being
            processed or are otherwise locked
        """
        pq = join(self._path, 'entries')
        pt = join(self._path, 'taken')
        if queued:
            entries = count_children(pq)
            if taken:
                return entries
            else:
                taken = count_children(pt)
                return entries - taken
        else:
            if taken:
                taken = count_children(pt)
                return taken
            else:
                raise AttributeError(
                    "You asked for an impossible situation.  Queue items are"
                    " waiting for a lock xor taken."
                    "  You cannot have queue entries"
                    " that are both not locked and not waiting.")


class Lock(BaseLock):
    def __init__(self, path):
        self._l = _zkLock(client=raw_client(), path=path)

    def acquire(self, blocking=True, timeout=None):
        return self._l.acquire(blocking=blocking, timeout=timeout)

    def release(self):
        if not self._l.release():
            raise UserWarning(
                "Cannot release() lock if you haven't called acquire()")


@util.cached
def raw_client():
    """Start a connection to ZooKeeper"""
    qb_zookeeper_hosts = get_NS().qb_zookeeper_hosts
    log.debug(
        "Connecting to ZooKeeper",
        extra=dict(qb_zookeeper_hosts=qb_zookeeper_hosts))
    zk = KazooClient(qb_zookeeper_hosts)
    zk.logger.handlers = log.handlers
    zk.logger.setLevel('WARN')
    zk.start()
    atexit.register(zk.stop)
    return zk


def delete(path, recursive=False):
    try:
        raw_client().delete(path, recursive=recursive)
    except NoNodeError as err:
        raise exceptions.NoNodeError(err)


def get(path):
    try:
        return raw_client().get(path)[0]
    except NoNodeError as err:
        raise exceptions.NoNodeError(err)


def get_children(path):
    try:
        return raw_client().get_children(path)
    except NoNodeError as err:
        raise exceptions.NoNodeError(err)


def count_children(path):
    try:
        return raw_client().get(path)[1].numChildren
    except NoNodeError as err:
        raise exceptions.NoNodeError(err)


def exists(path):
    return bool(raw_client().exists(path))


def set(path, value):
    return raw_client().set(path, value)


def create(path, value):
    try:
        return raw_client().create(path, value, makepath=True)
    except NodeExistsError as err:
        raise exceptions.NodeExistsError("%s: %s" % (path, err))


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--qb_zookeeper_hosts', help="The address to your Zookeeper cluster")
], description=(
    "Options that specify which queue to use to store state about your jobs")
)
