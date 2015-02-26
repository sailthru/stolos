"""
The configuration backend determines where the tasks configuration data is
stored.  For instance, configuration may be in a json file, or it may be in a
redis database.

This code doesn't know what is stored in the backend, but it does determine how
the data is stored.
"""
import logging
log = logging.getLogger('stolos.configuration_backend')

import stolos
from stolos import argparse_shared as at
from stolos.util import load_obj_from_path as _load_obj_from_path

# expose the base class to other configuration backend modules
from .tasks_config_base import TasksConfigBaseMapping, TasksConfigBaseSequence


_PREFIX = "stolos.configuration_backend"
# a lookup table that we can alias a particular
# configuration backend's full python import path to.
_KNOWN_DEFAULT_BACKENDS = {
    "json": "%s.%s" % (_PREFIX, "json_config.JSONMapping"),
    "redis": "%s.%s" % (_PREFIX, "redis_config.RedisMapping"),
}


def _load_backend(inpt):
    _cb = _KNOWN_DEFAULT_BACKENDS.get(inpt, inpt)
    try:
        cb = _load_obj_from_path(
            _cb, dict(key='configuration_backend', configuration_backend=_cb))
    except:
        log.error(
            "Could not load configuration backend",
            extra=dict(configuration_backend=_cb))
        raise
    return cb


build_arg_parser = at.build_arg_parser([at.group(
    "Application Dependency Configuration",
    at.add_argument(
        '--configuration_backend',
        default='json', type=_load_backend, help=(
            "Where do you store the application dependency data?"
            ' This option defines which backend to use to access'
            ' application dependency configuration.'
            ' See conf/stolos-env.sh for an example. '
            ' You can supply your own configuration backend or choose from the'
            ' following supported options: %s'
            ) % list(_KNOWN_DEFAULT_BACKENDS.keys())),
)])


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


def get_tasks_config():
    """
    Returns object to read Stolos application config from your chosen
    configuration backend.
    """
    ns = stolos.get_NS()
    return ns.configuration_backend()
