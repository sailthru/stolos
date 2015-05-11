import atexit
from kazoo.client import (
    KazooClient,
    Lock as _zkLock,
    LockingQueue as _zkLockingQueue
)
from kazoo.exceptions import (
    NoNodeError,
    NodeExistsError,
    NotEmptyError,
    LockTimeout,
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
        if not self._q.consume():
            raise UserWarning(
                "Cannot consume() from queue without first calling q.get()")

    def get(self, timeout=None):
        """Get an item from the queue or return None."""
        if timeout is None:
            timeout = get_NS().qb_zookeeper_timeout
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
        zk = raw_client()
        if queued:
            try:
                entries = zk.exists(pq).numChildren
            except AttributeError:
                return 0
            if taken:
                return entries
            else:
                taken = zk.exists(pt).numChildren
                return entries - taken
        else:
            if taken:
                try:
                    return zk.exists(pt).numChildren
                except AttributeError:
                    return 0
            else:
                raise AttributeError(
                    "You asked for an impossible situation.  Queue items are"
                    " waiting for a lock xor taken."
                    "  You cannot have queue entries"
                    " that are both not locked and not waiting.")

    def is_queued(self, value):
        """
        Return True if item is in queue or currently being processed.
        False otherwise
        """
        cli = raw_client()
        entries = self._q.structure_paths[1]
        # eek this is aweful
        try:
            items = {
                cli.get(join(entries, x))[0]
                for x in cli.get_children(entries)}
        except NoNodeError:
            items = []

        if value in items:
            return True
        return False


class Lock(BaseLock):
    def __init__(self, path):
        self._path = path
        self._l = _zkLock(client=raw_client(), path=path)

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        try:
            return self._l.acquire(blocking=blocking, timeout=timeout)
        except NoNodeError:
            raise UserWarning(
                "Timeout of %s seconds is too short to acquire lock" % timeout)
        except LockTimeout:
            return False

    def release(self):
        if not self._l.release():
            raise UserWarning(
                "Cannot release() lock if you haven't called acquire()")

    def is_locked(self):
        try:
            return bool(raw_client().exists(self._path).numChildren)
        except AttributeError:
            return False


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


def get(path):
    try:
        return raw_client().get(path)[0]
    except NoNodeError as err:
        raise exceptions.NoNodeError("%s: %s" % (path, err))


def exists(path):
    return bool(raw_client().exists(path))


def delete(path, _recursive=False):
    """Remove path from queue backend.

    `_recursive` - This is only for tests
    """
    try:
        raw_client().delete(path, recursive=_recursive)
        return True
    except (NoNodeError, NotEmptyError):
        return False


def set(path, value):
    try:
        return raw_client().set(path, value)
    except NoNodeError as err:
        raise exceptions.NoNodeError(
            "Must first create node before setting a new value. %s" % err)


def create(path, value):
    try:
        return raw_client().create(path, value, makepath=True)
    except NodeExistsError as err:
        raise exceptions.NodeExistsError("%s: %s" % (path, err))


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--qb_zookeeper_hosts', help="The address to your Zookeeper cluster"),
    at.add_argument(
        '--qb_zookeeper_timeout', default=5, type=float,
        help="Max num secs to wait on response if timeout not specified"),
], description=(
    "Options that specify which queue to use to store state about your jobs")
)
