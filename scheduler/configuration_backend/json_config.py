import os
import ujson

from . import TasksConfigBaseMapping, TasksConfigBaseSequence, log


def _getitem(self, key):
    rv = self.cache[key]
    if isinstance(rv, list):
        return JSONConfigSeq(rv)
    elif isinstance(rv, dict):
        return JSONConfig(rv)
    else:
        return rv


def _len(self):
    return len(self.cache)


class JSONConfig(TasksConfigBaseMapping):
    """
    A read-only dictionary loaded with data from a file identified by
    the environment variable, TASKS_JSON
    """
    def __init__(self, data=None):
        if data is None:
            try:
                fp = os.environ['TASKS_JSON']
            except KeyError:
                log.error(
                    "You must define TASKS_JSON if you use the JSONConfig"
                    " configuration backend")
                raise
            try:
                self.cache = ujson.load(open(fp))
            except:
                log.error("Failed to read json file.", extra={'fp': fp})
                raise
        else:
            assert isinstance(data, dict)
            self.cache = data

    __getitem__ = _getitem
    __len__ = _len

    def __iter__(self):
        return iter(self.cache)


class JSONConfigSeq(TasksConfigBaseSequence):
    def __init__(self, data):
        assert isinstance(data, list)
        self.cache = data

    __getitem__ = _getitem
    __len__ = _len
