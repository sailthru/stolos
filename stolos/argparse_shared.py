"""
We leverage argparse_tools to manage how arguments are passed in from
the command-line.  This file contains argparse options that may be shared.
"""
from argparse_tools import (
    build_arg_parser, add_argument, group, mutually_exclusive, lazy_kwargs,
    DefaultFromEnv)
import argparse


# This code block exists for linting
argparse
build_arg_parser
add_argument
group
mutually_exclusive
lazy_kwargs
DefaultFromEnv


def zookeeper_hosts(parser):
    parser.add_argument(
        '--zookeeper_hosts', action=DefaultFromEnv, env_prefix='STOLOS_')


@lazy_kwargs
def app_name(parser,
             help='The name of a task whose status is tracked with ZooKeeper',
             required=True, **kwargs):
    parser.add_argument(
        '-a', '--app_name', action=DefaultFromEnv, env_prefix='STOLOS_',
        help=help, required=required, **kwargs)
