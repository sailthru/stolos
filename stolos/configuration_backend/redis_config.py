"""
Fetch Stolos configuration from redis rather than from a json file.
"""
import os
import redis

from stolos.exceptions import _log_raise_if
from . import TasksConfigBaseMapping, _ensure_type, log
from .json_config import JSONMapping, JSONSequence


REDIS_PREFIX = 'stolos/'


class _RedisConfig(object):
    def __getitem__(self, key):
        if key not in self.cache:
            try:
                val = self.cli.hgetall('%s%s' % (REDIS_PREFIX, key))
            except:
                log.error((
                    "Redis failed to fetch app config data."
                    "Is the redis key you used an incorrect type?"
                    "  It should be a hash."), extra=dict(key=key))
                raise
            _log_raise_if(
                not val,
                "Given app_name does not exist in redis",
                dict(app_name=key), KeyError)

            # Convert redis values to python objects.  Potentially dangerous.
            val = {eval(k, {}, {}): eval(v, {}, {}) for k, v in val.items()}
            self.cache[key] = val
        return _ensure_type(self.cache[key], JSONMapping, JSONSequence)

    def __len__(self):
        return self.cli.dbsize()


class RedisMapping(_RedisConfig, TasksConfigBaseMapping):
    """
    Works like a read-only dictionary that fetches redis configuration for each
    configurated application.  Returns application config as a JSONMapping

    Each key: value in redis is of form:
        app_name: {python dict of app config stored as a k:v hashmap in redis)

    """
    def __init__(self, data=None,
                 db=int(os.environ.get('STOLOS_REDIS_DB', 0)),
                 host=os.environ.get('STOLOS_REDIS_HOST', 'localhost'),
                 port=int(os.environ.get('STOLOS_REDIS_PORT', 6379)),
                 **strictredis_connection_kwargs):
        self.db = db
        self.cli = redis.StrictRedis(
            db=db, host=host, port=port, **strictredis_connection_kwargs)
        if data is None:
            self.cache = {}
        elif isinstance(data, self.__class__):
            self.cli = data.cli
            self.cache = data.cache
        else:
            assert isinstance(data, dict), (
                "Oops! %s did not receive a dict" % self.__class__.__name__)
            self.cache = data

    def __iter__(self):
        return iter(
            app_name for app_name in self.cli.keys('%s*' % REDIS_PREFIX))


def set_config(app_name, app_conf, cli, delete_first=False):
    """
    A simple function to help you create redis config from a python dict.
    It intelligently stores string keys as strings and int keys as ints so
    that they can be eval'd on read.
    This mostly just serves as an example.

    If `delete_first` is True, remove the app config first

        >> app_name = 'myapp1'
        >> app_conf = {"job_type": "bash"}
        >> cli = redis.StrictRedis()
        >> set_config(app_name, app_conf, cli)
        [True]
        >> cli.hgetall("%s%s" % (REDIS_PREFIX, app_name))
        {"'job_type'": "'bash'"}

    """
    key = '%s%s' % (REDIS_PREFIX, app_name)
    with cli.pipeline(transaction=True) as pipe:
        if delete_first:
            pipe.delete(key)
        pipe.hmset(key, {repr(k): repr(v) for k, v in app_conf.items()})
        rv = pipe.execute()
    if not rv[-1]:
        raise Exception(
            "Failed to set app config data",
            extra=dict(app_name=app_name, app_config=app_conf))
    return rv
