import atexit
from kazoo.client import (
    KazooClient,
    Lock as _zkLock, LockingQueue as _zkLockingQueue
)
from os.path import join

from stolos import get_NS
from stolos import argparse_shared as at
from stolos import util
from .baseapi import BaseLock, BaseLockingQueue
from . import log


class LockingQueue(BaseLockingQueue):
    def __init__(self, path):
        self._path = path
        self._q = _zkLockingQueue(client=get_client(), path=path)

    def put(self, value, priority=100):
        self._q.put(value, priority=priority)

    def consume(self):
        if not self._q.consume():
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
        self._l = _zkLock(client=get_client(), path=path)

    def acquire(self, blocking=True, timeout=None):
        return self._l.acquire(blocking=blocking, timeout=timeout)

    def release(self):
        if not self._l.release():
            raise UserWarning(
                "Cannot release() lock if you haven't called acquire()")


@util.cached
def get_client():
    """Start a connection to ZooKeeper"""
    zookeeper_hosts = get_NS().zookeeper_hosts
    log.debug(
        "Connecting to ZooKeeper",
        extra=dict(zookeeper_hosts=zookeeper_hosts))
    zk = KazooClient(zookeeper_hosts)
    zk.logger.handlers = log.handlers
    zk.logger.setLevel('WARN')
    zk.start()
    atexit.register(zk.stop)
    return zk


def get(self, path):
    return get_client().get(path)[0]


def get_children(self, path):
    return get_client().get_children(path)


def count_children(self, path):
    return get_client().get(path)[1].numChildren


def exists(self, path):
    return get_client().exists(path)


def set(self, path, value):
    return get_client().set(path, value)


def create(self, path, value, makepath=False):
    return get_client.create(path, value, makepath=makepath)


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--zookeeper_hosts', help="The address to your Zookeeper cluster")
], description=(
    "Options that specify which queue to use to store state about your jobs")
)
