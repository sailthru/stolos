"""
Plugins enable different types of tasks to be run Stolos.

Everything needed to create a stolos plugin is available here.
"""

# publicly visible to plugins
from stolos import argparse_shared as at
from stolos.configuration_backend import (
    TasksConfigBaseMapping, TasksConfigBaseSequence)
TasksConfigBaseSequence, TasksConfigBaseMapping
from stolos import api
at, api


# imports hidden from plugins
from logging import getLogger as _getLogger
log = _getLogger('stolos.plugins')
from stolos.exceptions import CodeError as _CodeError


def log_and_raise(err, log_details):
    """The less unexpected way for plugins to fail.

    A helper function that logs the given exception
    and then raises an exception.  Stolos will see this error, mark the
    job as failed and quit.  Plugin exceptions not handled by this function
    will cause Stolos to complain that you have unexpected errors in your
    plugin code.
    """
    log.exception(err, extra=log_details)
    raise _CodeError(
        'Task failed. This particular error message will never appear in logs.'
    )
