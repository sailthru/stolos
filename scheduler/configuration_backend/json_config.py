import os
import ujson

from . import TasksConfigBase


class JSONConfig(TasksConfigBase):
    """
    A read-only dictionary loaded with data from a file identified by
    the environment variable, TASKS_JSON
    """
    def __init__(self, data=None):
        if data is None:
            self.cache = ujson.load(open(os.environ['TASKS_JSON']))
        else:
            self.cache = data

    def __getitem__(self, key):
        rv = self.cache[key]
        if isinstance(rv, list):
            return tuple(rv)
        elif isinstance(rv, dict):
            return JSONConfig(rv)
        else:
            return rv

    def __len__(self):
        return len(self.cache)

    def __iter__(self):
        return iter(self.cache)
