"""
We leverage argparse_tools to manage how arguments are passed in from
the command-line.  This file contains argparse options that may be shared.
"""
from argparse_tools import (
    build_arg_parser as _build_arg_parser,
    group, mutually_exclusive, lazy_kwargs,
    DefaultFromEnv,
    add_argument as _add_argument)

from stolos import log
from stolos import util

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
    if 'prog' not in kwargs:
        kwargs['prog'] = 'stolos'
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
            kwargs['const'] = True
            kwargs['nargs'] = '?'
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


def _load_backend(known_backends, backend_type):
    """
    Returns a function that will load a given backend.

    `known_backends` - dict defining all Stolos-supported backends in form:
        {'easy-to-type-backend-name': 'python.path.to.class', ...}
    `backend_type` - either "configuration" or "queue"
    """
    def _load_backend_decorator(inpt):
        _backend = known_backends.get(inpt, inpt)
        try:
            backend = util.load_obj_from_path(
                _backend,
                {'key': '%s_backend' % backend_type, backend_type: _backend})
        except Exception as err:
            log.error(
                "Could not load %s backend. err: %s" % (backend_type, err),
                extra={'%s_backend' % backend_type: _backend})
            raise
        return backend
    return _load_backend_decorator


def backend(backend_type, default, known_backends, help):
    """
    adds the option --<backend_type>_backend with given `default`, where
    the `type=` function loads the chosen backend as a python object

    Only 2 backends are supported:  --configuration_backend and --queue_backend

    `backend_type` - either "configuration" or "queue"
    `known_backends` - Stolos-supported backends for the given `backend_type`
    `default` - the argparse default= option
    `help` - the argparse help= option

    """
    if backend_type not in ['configuration', 'queue']:
        raise UserWarning(
            "The only recognized types of backends are"
            " 'configuration' and 'queue'")

    return add_argument(
        '--%s_backend' % backend_type,
        default=default,
        type=_load_backend(known_backends, backend_type),
        help=help.format(known_backends=str(known_backends.keys())))
