from stolos import argparse_shared as at

import logging
log = logging.getLogger('stolos.queue_backend')


from .shared import get_job_path, get_qbclient, get_lock_path
get_job_path, get_qbclient, get_lock_path

from .modify_job_state import (
    maybe_add_subtask, readd_subtask, set_state, inc_retry_count,
    ensure_parents_completed,
    _set_state_unsafe  # TODO: get rid of _set_state_unsafe
)
maybe_add_subtask, readd_subtask, set_state, inc_retry_count,
ensure_parents_completed, _set_state_unsafe

from .read_job_state import (check_state)
check_state

from .locking import (obtain_execute_lock, is_execute_locked)
obtain_execute_lock, is_execute_locked

from .qbcli_baseapi import Lock as BaseLock
BaseLock


build_arg_parser = at.build_arg_parser([at.group(
    "Stolos Queue Backend (manages job state)",
    at.backend(
        backend_type='queue',
        default='zookeeper',
        known_backends={
            "zookeeper": "stolos.queue_backend.qbcli_zookeeper",
            "majorityredis": "stolos.queue_backend.qbcli_majorityredis"},
        help=(
            'Select a database that stores job state.'
            ' This option defines which queue backend Stolos uses.'
            ' You can supply your own queue backend or choose from the'
            ' following supported options: {known_backends}')),
)])
