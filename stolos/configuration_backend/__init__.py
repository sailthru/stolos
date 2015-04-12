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

# expose the base class to other configuration backend modules
from .tasks_config_base import TasksConfigBaseMapping, TasksConfigBaseSequence
TasksConfigBaseMapping, TasksConfigBaseSequence


build_arg_parser = at.build_arg_parser([at.group(
    "Application Dependency Configuration",
    at.backend(
        backend_type='configuration',
        default='json',
        known_backends={
            "json": "stolos.configuration_backend.json_config.JSONMapping",
            "redis": "stolos.configuration_backend.redis_config.RedisMapping"},
        help=(
            "Where do you store the application dependency data?"
            ' This option defines which configuration backend Stolos uses'
            ' to access the directed graph defining how your applications'
            ' depend on each other.'
            ' You can supply your own configuration backend or choose from the'
            ' following supported options: {known_backends}')),
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
