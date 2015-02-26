import simplejson

from . import (
    TasksConfigBaseMapping, TasksConfigBaseSequence, log,
    _ensure_type)

from stolos import argparse_shared as at
from stolos import get_NS


build_arg_parser = at.build_arg_parser([at.group(
    "Configuration Backend Options: JSON",
    at.add_argument(
        '--tasks_json', required=True, help=(
            "Filepath to a json file defining Stolos application config")),
)])


class _JSONMappingBase(object):
    def __getitem__(self, key):
        return _ensure_type(
            self.cache[key], JSONMapping, JSONSequence)

    def __len__(self):
        return len(self.cache)


class JSONMapping(_JSONMappingBase, TasksConfigBaseMapping):
    """
    A read-only dictionary loaded with data from a file identified by
    the --tasks_json option
    """
    def __init__(self, data=None):
        if data is None:
            try:
                fp = get_NS().tasks_json
            except KeyError:
                log.error((
                    "You must define --tasks_json if you use the %s"
                    " configuration backend") % self.__class__.__name__)
                raise
            try:
                self.cache = simplejson.load(open(fp))
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
