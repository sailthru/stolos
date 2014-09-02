import os
import ujson

from . import TasksConfigBase, log


class JSONConfig(TasksConfigBase):
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
