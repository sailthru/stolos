# TODO this module
import redis

from stolos import get_NS
from stolos import argparse_shared as at

from .qbcli_baseapi import Lock as BaseLock, LockingQueue as BaseLockingQueue


class Lock(BaseLock):
    pass


class LockingQueue(BaseLockingQueue):
    pass


def raw_client(self):
    NS = get_NS()
    return redis.StrictRedis(
        NS.qb_redis_host, NS.qb_redis_port, NS.qb_redis_db)


build_arg_parser = at.build_arg_parser([
    at.add_argument('--qb_redis_host', default='127.0.0.1'),
    at.add_argument('--qb_redis_port', default='6379'),
    at.add_argument('--qb_redis_db', default='0'),
], description=(
    "These options specify which queue to use to store state about your jobs"))
