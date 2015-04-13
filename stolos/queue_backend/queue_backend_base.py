"""
This file defines the basic api and classes that a queue backend must implement.
All queue backends should define these functions and inherit from these classes
"""


def get_client():
    raise NotImplementedError

def get(path):
    # or raise NoNodeError
    raise NotImplementedError

def get_children(path):
    # or raise NoNodeError
    raise NotImplementedError

def exists(path):
    raise NotImplementedError

def set(path, value):
    raise NotImplementedError

def create(path, value, makepath=False):
    raise NotImplementedError

def qsize(queued=True, taken=True):
    raise NotImplemented()


class BaseLock(object):
    def __init__(self, client, path):
        raise NotImplemented()

    def acquire(self, blocking=True, timeout=None):
        raise NotImplemented()

    def release(self):
        raise NotImplemented()


class BaseLockingQueue(object):
    def __init__(self, client, path):
        self._client = client
        self._path = path

    def put(self, value, priority=100):
        raise NotImplemented()

    def consume(self):
        raise NotImplemented()

    def get(self, timeout):
        raise NotImplemented()

    def is_queued(self, value):
        raise NotImplemented()
