# TODO this module
import redis

from stolos import get_NS

# from .queue_backend_base import


class RedisQB(object):
    pass

    def __init__(self):
        NS = get_NS()
        self._cli = redis.StrictRedis(
            NS.queue_redis_host, NS.queue_redis_port, NS.queue_redis_db)
