from stolos import argparse_shared as at

import logging
log = logging.getLogger('stolos.queue_backend')

# TODO:
# from .reads_job_state import ()
# from .modifies_job_state import ()


build_arg_parser = at.build_arg_parser([at.group(
    "Stolos Queue Backend (manages job state)",
    at.backend(
        backend_type='queue',
        default='zookeeper',
        known_backends={
            "zookeeper": "stolos.queue_backend.qb_zookeeper.ZookeeperQB",
            "redis": "stolos.queue_backend.qb_redis.RedisQB"},
        help=(
            'Select a database that stores job state.'
            ' This option defines which queue backend Stolos uses.'
            ' You can supply your own queue backend or choose from the'
            ' following supported options: {known_backends}')),
)])
