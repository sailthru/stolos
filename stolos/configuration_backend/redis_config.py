"""
Fetch Stolos configuration from redis rather than from a json file.
"""
import redis

from stolos import get_NS
from stolos.exceptions import _log_raise_if
from . import TasksConfigBaseMapping, _ensure_type, log
from .json_config import JSONMapping, JSONSequence

from stolos import argparse_shared as at


build_arg_parser = at.build_arg_parser([at.group(
    ("Configuration Backend Options: Redis"
     "  By default, assume the Redis DB is available locally"),
    at.add_argument(
        '--redis_key_prefix', default='stolos/', help=(
            "All redis keys stolos creates are prefixed by this value")),
    at.add_argument(
        '--redis_db', default=0, type=int,
        help="Number of the DB that redis connects to"),
    at.add_argument(
        '--redis_host', default='localhost',
        help="Host address to redis server"),
    at.add_argument(
        '--redis_port', default=6379, type=int,
        help="Port to connect to redis server at"),
    at.add_argument(
        '--redis_connection_opts', type=lambda x: x.split('='), help=(
            "Additional arguments to pass to redis.StrictRedis")),
)])


class _RedisConfig(object):
    def __getitem__(self, key):
        if key not in self.cache:
            key = "%s%s" % (self.redis_key_prefix, key)
            try:
                val = self.cli.hgetall(key)
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
            self.cache[key] = _ensure_type(val, JSONMapping, JSONSequence)
        return self.cache[key]

    def __len__(self):
        return self.cli.dbsize()


class RedisMapping(_RedisConfig, TasksConfigBaseMapping):
    """
    Works like a read-only dictionary that fetches redis configuration for each
    configurated application.  Returns application config as a JSONMapping

    Each key: value in redis is of form:
        app_name: {python dict of app config stored as a k:v hashmap in redis)

    """
    def __init__(self, data=None):
        NS = get_NS()
        self.db = NS.redis_db
        self.redis_key_prefix = NS.redis_key_prefix
        self.cli = redis.StrictRedis(
            db=NS.redis_db, port=NS.redis_port, host=NS.redis_host)
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
            app_name for app_name in self.cli.keys(
                '%s*' % self.redis_key_prefix))


def set_config(app_name, app_conf, cli,
               delete_first=False, redis_key_prefix=""):
    """
    A simple function to help you create redis config from a python dict.
    It intelligently stores string keys as strings and int keys as ints so
    that they can be eval'd on read.
    This mostly just serves as an example.

    `delete_first` (bool) if True, remove the app config first
    `redis_key_prefix` if redis keys used by Stolos have a prefix, specify it

        >> app_name = 'myapp1'
        >> app_conf = {"bash_cmd": "echo 123"}
        >> cli = redis.StrictRedis()
        >> set_config(app_name, app_conf, cli, delete_first=True)
        [True]
        >> cli.hgetall("%s%s" % ('', app_name))
        {"'bash_cmd'": "'echo 123'"}

    """
    key = '%s%s' % (redis_key_prefix, app_name)
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
