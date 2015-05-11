from stolos import argparse_shared as _at


class LockingQueue(object):
    def __init__(self, path):
        raise NotImplemented()

    def put(self, value, priority=100):
        """Add item onto queue.
        Rank items by priority.  Get low priority items before high priority
        """
        # TODO: Return values?
        raise NotImplemented()

    def consume(self):
        """Consume value gotten from queue.
        Raise UserWarning if consume() called before get()
        """
        raise NotImplemented()

    def get(self, timeout=None):
        """Get an item from the queue or return None.  Do not block forever."""
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


class Lock(object):
    def __init__(self, path):
        self._path = path

    def acquire(self, blocking=False, timeout=None):
        """
        Acquire a lock at the Lock's path.
        Return True if acquired, False otherwise

        `blocking` (bool) If False, return immediately if we got lock.
            If True, wait up to `timeout` seconds to acquire a lock
        `timeout` (int) number of seconds.  By default, wait indefinitely
        """
        # TODO: check return values & errors
        raise NotImplemented()

    def release(self):
        """
        Release a lock at the Lock's path.
        Return True if success.  Raise UserWarning otherwise, possibly due to:
            - did not release a lock
            - lock already released
            - lock does not exist (perhaps it was never acquired)
        """
        # TODO: check return values & errors
        raise NotImplemented()

    def is_locked(self):
        """
        Return True if path is currently locked by anyone, and False otherwise
        """
        raise NotImplemented()


def get(path):
    """Get value at given path.
    If path does not exist, throw stolos.exceptions.NoNodeError
    """
    raise NotImplementedError()


def exists(path):
    """Return True if path exists (value can be ''), False otherwise"""
    raise NotImplementedError()


def delete(path, _recursive=False):
    """Remove path from queue backend.

    `_recursive` - This is only for tests
    """
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


build_arg_parser = _at.build_arg_parser([])
