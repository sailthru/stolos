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

# expose the configuration backend base class for developers
from .tasks_config_base import TasksConfigBaseMapping, TasksConfigBaseSequence


build_arg_parser = at.build_arg_parser([at.group(
    # TODO: inherit from the configuration backend choice somehow?
    "Application Dependency Configuration",
    at.add_argument(
        '--configuration_backend',
        default='stolos.configuration_backend.json_config.JSONMapping', help=(
            "Where do you store the application dependency data?"
            ' This options defines which backend to use to access'
            ' application dependency configuration.'
            ' Stolos supports a couple options.'
            ' See conf/stolos-env.sh for an example')),
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
    try:
        cb = _load_obj_from_path(
            ns.configuration_backend,
            dict(key='configuration_backend',
                 configuration_backend=ns.configuration_backend)
        )
    except:
        log.error("Could not load configuration backend")
        raise
    return cb()
