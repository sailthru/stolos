"""
Stolos can be invoked in one of two ways:
    - as an application runner, stolos starts from the command-line
    - as an api, stolos gets imported and run like code.

In both scenarios, Stolos may need users to define specific configuration
options.  This module provides a uniform way for the api and runner to
access configuration options.

Users of Stolos's api should refer directly to stolos.api rather than accessing
this module directly
"""
from stolos import (
    argparse_tools as at,
    dag_tools as dt,
    configuration_backend as cb,
    zookeeper_tools as zkt
)

# get a parser
# load required env vars

# from runner, this is easy because the scope of env vars needed is fully
#     specified.
# from api, this is not fully specified because scope of env vars needed is
#     unknown
at.build_arg_parser(
    parents=[x.build_arg_parser() for x in (dt, cb)])()
