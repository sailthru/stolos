import os
import ujson

from . import (
    TasksConfigBaseMapping, TasksConfigBaseSequence, log,
    _ensure_type)


class _JSONMappingBase(object):
    def __getitem__(self, key):
        return _ensure_type(
            self.cache[key], JSONMapping, JSONSequence)

    def __len__(self):
        return len(self.cache)


class JSONMapping(_JSONMappingBase, TasksConfigBaseMapping):
    """
    A read-only dictionary loaded with data from a file identified by
    the environment variable, TASKS_JSON
    """
    def __init__(self, data=None):
        if data is None:
            try:
                fp = os.environ['TASKS_JSON']
            except KeyError:
                log.error((
                    "You must define TASKS_JSON if you use the %s"
                    " configuration backend") % self.__class__.__name__)
                raise
            try:
                self.cache = ujson.load(open(fp))
            except:
                log.error("Failed to read json file.", extra={'fp': fp})
                raise
        elif isinstance(data, self.__class__):
            self.cache = data.cache
        else:
            assert isinstance(data, dict), (
                "Oops! %s did not receive a dict" % self.__class__.__name__)
            self.cache = data

    def __iter__(self):
        return iter(self.cache)


class JSONSequence(_JSONMappingBase, TasksConfigBaseSequence):
    def __init__(self, data):
        assert isinstance(data, list)
        self.cache = data
