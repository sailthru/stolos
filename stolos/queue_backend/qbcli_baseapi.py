from stolos import argparse_shared as _at


class LockingQueue(object):
    def __init__(self, path):
        raise NotImplemented()

    def put(self, value, priority=100):
        raise NotImplemented()

    def consume(self):
        raise NotImplemented()

    def get(self, timeout=None):
        raise NotImplemented()

    def size(self, queued=True, taken=True):
        """
        Find the number of jobs in the queue

        `queued` - Include the entries in the queue that are not currently
            being processed or otherwise locked
        `taken` - Include the entries in the queue that are currently being
            processed or are otherwise locked
        """
        raise NotImplemented()


class Lock(object):
    def acquire(self, blocking=True, timeout=None):
        raise NotImplemented()

    def release(self):
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
    raise NotImplementedError()


def exists(path):
    raise NotImplementedError()


def set(path, value):
    raise NotImplementedError()


def create(path, value, makepath=False):
    raise NotImplementedError()


build_arg_parser = _at.build_arg_parser([])
