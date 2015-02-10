"""
We leverage argparse_tools to manage how arguments are passed in from
the command-line.  This file contains argparse options that may be shared.
"""
from argparse_tools import (
    build_arg_parser as _build_arg_parser,
    group, mutually_exclusive, lazy_kwargs,
    DefaultFromEnv,
    add_argument as _add_argument)


# This code block exists for linting
group
mutually_exclusive
lazy_kwargs
DefaultFromEnv


def build_arg_parser(*args, **kwargs):
    """Wraps at.build_arg_parser to set some defaults

    disable --help by default"""
    if 'add_help' not in kwargs:
        kwargs['add_help'] = False
    return _build_arg_parser(*args, **kwargs)


def add_argument(*args, **kwargs):
    """Wraps argparse.ArgumentParser.add_argument to guarantee that all
    defined options are available from environment variables prefixed by
    "STOLOS_"

    add_argument('--booloption', action='store_true')
    """
    if 'action' in kwargs:
        val = kwargs.pop('action')
        if val == 'store_true':
            kwargs['type'] = bool
        elif val == 'store_false':
            def notbool(x):
                return not bool(x)
            kwargs['type'] = notbool
        else:
            raise NotImplemented(
                "Not sure how to deal with this argparse argument option"
                "  Stolos applies a custom action to get defaults from"
                " the environment.  You cannot specify action=%s for the"
                " argument identified by: %s" % (val, str(args[:2])))
    return _add_argument(
        *args, action=DefaultFromEnv, env_prefix='STOLOS_', **kwargs)


@lazy_kwargs
def app_name(parser,
             help='The name of a task whose status is tracked with ZooKeeper',
             required=True, **kwargs):
    add_argument(
        '-a', '--app_name', help=help, required=True, **kwargs)(parser)
