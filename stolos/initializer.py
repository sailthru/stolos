"""
Stolos can be invoked in one of two ways:
    - as an application runner, stolos starts from the command-line
    - as an api, stolos gets imported and run like code.

In both scenarios, Stolos may need users to define specific configuration
options.  This module provides a uniform way for the api and runner to
access configuration options.

Users of Stolos's api should refer directly to stolos.api rather than accessing
this module directly

Developer note:  The only code that should import from this file is
`stolos.api` and `stolos.runner`.
"""
import argparse
import stolos
from stolos import argparse_shared as at
from stolos import log


def _get_parent_parsers(modules)
    """Internal function to call m.build_arg_parser() for each m in modules"""
    for m in set(modules):
        p = m.build_arg_parser()
        if not isinstance(p, argparse.ArgumentParser):
            msg = (
                "Failed to initialize Stolos because the initializer"
                " epected an instance of argparse.ArgumentParser but received"
                " something else")
            log.error(msg, extra=dict(parent_argparser=p))
            raise TypeError("%s.  Received: %s" % (msg, type(p)))
        if p in parents:
            log.warn(
                "While trying to initialize Stolos, you defined the same"
                " parent argparser more than once.  See details for the module"
                " this error occurred in",
                extra=dict(module=m))
            continue
        yield p


def initialize(modules, args=None, **argument_parser_kwargs):
    """
    Initialize Stolos such that we ensure all required configuration settings
    are unified in one central place before we do anything with Stolos.

    `modules` - is a list of (Stolos) modules containing a callable
        build_arg_parser function.  The initializer will set `module.NS`.
        If any modules define conflicting argument options, an error will raise
    `args` - if supplied, is passed to the parse_args(...) function of an
        argparse.ArgumentParser.  This should default to sys.argv.
        To guarantee no arguments are read from sys.argv, set args=[]
        Otherwise, you can set args=['--option1', 'val', ...]
    `argument_parser_kwargs` - if any, passed to the ArgumentParser
    """

    build_arg_parser = at.build_arg_parser(
        description="Initialize Stolos, whether running it or calling its api",
        parents=_get_parent_parsers(modules),
        add_help=False,
        **argument_parser_kwargs)

    if args is not None:
        ns = build_arg_parser.parse_args(args)
    else:
        ns = build_arg_parser.parse_args()
    del stolos.Uninitialized
    for m in modules:
        setattr(m, 'NS', ns)
    return ns
