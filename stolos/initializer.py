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
import importlib
import stolos
from stolos import argparse_shared as at
from stolos import util
from stolos import log


def _get_parent_parsers(objects):
    """Internal function to call m.build_arg_parser() for each m in objects"""
    for m in set(objects):
        if isinstance(m, argparse.ArgumentParser):
            p = m
        else:
            p = m.build_arg_parser()
        if not isinstance(p, argparse.ArgumentParser):
            msg = (
                "Failed to initialize Stolos because the initializer"
                " epected an instance of argparse.ArgumentParser but received"
                " something else")
            log.error(msg, extra=dict(parent_argparser=p))
            raise TypeError("%s.  Received: %s" % (msg, type(p)))
        yield p


def initialize_configuration_backend(choice, parser, add_help):
    """
    get options for the chosen configuration_backend.
    ensure they don't conflict with previously defined ones
    """
    cb = importlib.import_module(
        util.load_obj_from_path(choice).__module__)\
        .build_arg_parser()
    newparser = at.build_arg_parser(parents=[parser, cb], add_help=add_help)
    return newparser


def initialize(objects, args=None, add_help=True, **argument_parser_kwargs):
    """
    Initialize Stolos such that we ensure all required configuration settings
    are unified in one central place before we do anything with Stolos.
    Raises error if any parsers define conflicting argument options.

    All internal stolos libraries should call to stolos.get_NS() when they need
    to access configuration.

    `objects` - is a list of build_arg_parser functions or objects
        (ie Stolos modules) containing a callable build_arg_parser function.
    `args` - if supplied, is passed to the parse_args(...) function of an
        argparse.ArgumentParser.  Default to sys.argv.
        To guarantee NO arguments are read from sys.argv, set args=[]
        Otherwise, you can set args=['--option1', 'val', ...]
    `add_help` - if False, assumes you will post-process the argument parser
        and add a --help option later.  This also causes the option parser to
        parse_known_args and ignore arguments it doesn't recognize (ie "-h").
    `argument_parser_kwargs` - if any, passed to the ArgumentParser
    """
    # partially initialize a parser to get selected configuration backend
    parser = at.build_arg_parser(
        description="Initialize Stolos, whether running it or calling its api",
        parents=list(_get_parent_parsers(objects)),
        **argument_parser_kwargs)
    if args is not None:
        ns, _ = parser.parse_known_args(args)
    else:
        ns, _ = parser.parse_known_args()
    # get a new parser updated with options for chosen configuration backend
    parser = initialize_configuration_backend(
        ns.configuration_backend, parser, add_help)

    if add_help:
        ns = parser.parse_args()
    else:
        ns, _ = parser.parse_known_args()
    stolos.NS = ns
    try:
        del stolos.Uninitialized
    except AttributeError:
        log.warn(
            "Stolos was re-initialized.  You may have imported the api and"
            " then done something weird like re-import it or manually"
            " call Stolos's initializer."
        )
    return parser, ns
