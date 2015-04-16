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
from stolos import log


def _get_parent_parsers(objects):
    """Internal function to call m.build_arg_parser() for each m in objects"""
    for m in set(objects):
        if not isinstance(m, argparse.ArgumentParser):
            p = m.build_arg_parser()
            if not isinstance(p, argparse.ArgumentParser):
                msg = (
                    "Failed to initialize Stolos because the initializer"
                    " expected an instance of argparse.ArgumentParser but"
                    " received something else")
                log.error(msg, extra=dict(unrecognized_object=p))
                raise TypeError("%s.  Received: %s" % (msg, type(p)))
        else:
            p = m
        yield p


def initialize_backend(backend, parser, add_help):
    """
    get options for the chosen backend
    ensure they don't conflict with previously defined ones
    """
    if hasattr(backend, '__module__'):
        # ie. configuration_backends are classes
        # in a module containing the arg parser
        # (otherwise, assume the backend is a module containing the arg parser)
        backend = importlib.import_module(backend.__module__)
    newparser = at.build_arg_parser(
        parents=[parser, backend.build_arg_parser()], add_help=add_help)
    return newparser


def initialize(objects, args=None, parse_known_args=False,
               **argument_parser_kwargs):
    """
    Initialize Stolos such that we ensure all required configuration settings
    are unified in one central place before we do anything with Stolos.
    Raises error if any parsers define conflicting argument options.

    This function is called by user-facing or application-level code.
    All internal stolos libraries should call to stolos.get_NS() when they need
    to access configuration.  Internal libraries should not call this function.

    Returns (argparse.ArgumentParser(...), argparse.Namespace(...))

    `objects` - is a list of build_arg_parser functions or objects
        (ie Stolos modules) containing a callable build_arg_parser attribute.
    `args` - (optional).  Define command-line arguments to use.
        Default to sys.argv (which is what argparse does).
        Explicitly pass args=[] to not read command-line arguments, and instead
        expect that all arguments are passed in as environment variables.
        To guarantee NO arguments are read from sys.argv, set args=[]
        Example:  args=['--option1', 'val', ...]
    `parse_known_args` - if True, parse only known commandline arguments and
        do not add_help (ie don't recognize '-h').  Assume you will
        post-process the argument parser and add a --help option later.
        If False, add_help (ie recognize '-h') and fail if anything on
        command-line is not recognized by the argument parser
    `argument_parser_kwargs` - (optional) passed to the ArgumentParser(...)
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

    # get a new parser updated with options for each chosen backend
    parser = initialize_backend(
        ns.configuration_backend, parser, add_help=False)
    parser = initialize_backend(
        ns.queue_backend, parser, add_help=not bool(parse_known_args))

    if not parse_known_args:
        ns = parser.parse_args(args)
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
