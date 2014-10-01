import logging
log = logging.getLogger('scheduler.configuration_backend')

# expose the configuration backend base class for developers
from .tasks_config_base import TasksConfigBaseMapping, TasksConfigBaseSequence


def _ensure_type(value, mapping_kls, seq_kls):
    """
    Code that backends can use to ensure dict and list values they may
    attempt to return are always one of two expected
    types.  Values that aren't dicts or lists are just returned as-is

    `mapping_kls` - receives a dict and returns an instance of some class
    `seq_kls` - receives a list and returns an instance of some class
    `value` - an object
    """
    if isinstance(value, list):
        return seq_kls(value)
    elif isinstance(value, dict):
        return mapping_kls(value)
    else:
        return value
