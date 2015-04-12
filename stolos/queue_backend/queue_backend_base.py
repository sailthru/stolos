class BaseQueueBackend(object):
    def __init__(self):  # TODO
        raise NotImplementedError

    def get(self, path):
        # or raise NoNodeError
        raise NotImplementedError

    def get_children(self, path):
        # or raise NoNodeError
        raise NotImplementedError

    def exists(self, path):
        raise NotImplementedError

    def set(self, path, value):
        raise NotImplementedError

    def create(self, path, value, makepath=False):
        raise NotImplementedError

    def qsize(self, queued=True, taken=True):
        raise NotImplemented()

    def Lock(self, path):
        # acquire, release
        raise NotImplementedError

    def LockingQueue(path):
        # put, consume, get, is_queued
        raise NotImplementedError


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
