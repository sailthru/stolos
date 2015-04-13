# TODO all of this
import atexit
from kazoo.client import (
    KazooClient,
    # Lock as _Lock, LockingQueue as _LockingQueue
)

from stolos import get_NS
from stolos import argparse_shared as at
from .queue_backend_base import BaseLock, BaseLockingQueue
from . import log


class ZookeeperLockingQueue(BaseLockingQueue):
    def __init__(self, client, path):
        raise NotImplemented()

    def put(self, value, priority=100):
        raise NotImplemented()

    def consume(self):
        raise NotImplemented()

    def get(self, timeout):
        raise NotImplemented()

    def is_queued(self, value):
        raise NotImplemented()


class ZookeeperLock(BaseLock):
    def __init__(self, client, path):
        raise NotImplemented()

    def acquire(self, blocking=True, timeout=None):
        raise NotImplemented()

    def release(self):
        raise NotImplemented()


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
    raise NotImplementedError


def get_children(self, path):
    raise NotImplementedError


def exists(self, path):
    raise NotImplementedError


def set(self, path, value):
    raise NotImplementedError


def create(self, path, value, makepath=False):
    raise NotImplementedError


def qsize(self, queued=True, taken=True):
    raise NotImplemented()


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--zookeeper_hosts', help="The address to your Zookeeper cluster")
], description=(
    "Options that specify which queue to use to store state about your jobs")
)
