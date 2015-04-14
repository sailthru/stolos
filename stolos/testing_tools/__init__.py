"""
Useful utilities for testing
"""

from .queue_backend_validations import (
    enqueue,
    validate_zero_queued_task,
    validate_zero_completed_task,
    validate_one_failed_task,
    validate_one_queued_executing_task,
    validate_one_queued_task,
    validate_one_completed_task,
    validate_one_skipped_task,
    validate_n_queued_task,
    cycle_queue,
    consume_queue,
    get_qb_status,
)
enqueue, validate_zero_queued_task, validate_zero_completed_task,
validate_one_failed_task, validate_one_queued_executing_task,
validate_one_queued_task, validate_one_completed_task,
validate_one_skipped_task, validate_n_queued_task,
cycle_queue, consume_queue, get_qb_status,

# let tests configure their own setup and teardown
from setup_funcs import with_setup_factory

# offer some reasonable pre-set defaults
from setup_funcs import (
    inject_into_dag,
    default_with_setup as with_setup,
)
inject_into_dag, with_setup, with_setup_factory
