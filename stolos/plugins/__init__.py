"""
Plugins enable different types of tasks to be run Stolos.

Everything needed to create a stolos plugin is available here.
"""

# publicly visible to plugins
from stolos import argparse_tools as at
from stolos import api


# imports hidden from plugins
from logging import getLogger as _getLogger
log = _getLogger('stolos.plugins')
from stolos.exceptions import CodeError as _CodeError
from stolos import runner as _runner


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


# the parent parser that plugins implicitly inherit from
# As a plugin developer, you could technically use this instead of
# build_plugin_arg_parser if you know what you're doing, but we don't
# recommend it or promise that this will be here
_parent_parser = _runner._build_arg_parser()


def build_plugin_arg_parser(*args, **kwargs):
    """All plugins should use this function
    to define a variable called `build_arg_parser`

    It guarantees that your plugin options inherit the argparse options
    that Stolos defines, and it guarantees that your plugin's options
    do not conflict with options necessary to run other components of Stolos.

    For instance:

    >>> from stolos import argparse_shared as at
    >>> build_arg_parser = plugins.build_plugin_arg_parser([at.group(
        "My plugin's argparse options",
        at.add_argument(
            '--optionA', default=123,
            help="Defined when starting stolos from commandline")
        )])
    """
    return at.build_arg_parser(
        *args,
        parents=kwargs.pop('parents', [_parent_parser]),
        add_help=kwargs.pop('add_help', True),
        **kwargs)
