from stolos import argparse_shared as at
from stolos import get_NS

import logging
log = logging.getLogger('stolos.queue_backend')


from .modify_job_state import (maybe_add_subtask, readd_subtask)
maybe_add_subtask, readd_subtask

from .read_job_state import (check_state)
check_state


build_arg_parser = at.build_arg_parser([at.group(
    "Stolos Queue Backend (manages job state)",
    at.backend(
        backend_type='queue',
        default='zookeeper',
        known_backends={
            "zookeeper": "stolos.queue_backend.qbcli_zookeeper",
            "redis": "stolos.queue_backend.qbcli_redis"},
        help=(
            'Select a database that stores job state.'
            ' This option defines which queue backend Stolos uses.'
            ' You can supply your own queue backend or choose from the'
            ' following supported options: {known_backends}')),
)])


def get_qbclient():
    return get_NS().queue_backend
