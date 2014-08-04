import os
from argparse_tools import (
    build_arg_parser, add_argument, group, mutually_exclusive, lazy_kwargs)
import argparse


# This code block exists for linting
argparse
build_arg_parser
add_argument
group
mutually_exclusive
lazy_kwargs


def zookeeper_hosts(parser):
    parser.add_argument(
        '--zookeeper_hosts', default=os.environ["ZOOKEEPER_HOSTS"])


@lazy_kwargs
def app_name(parser,
             help='The name of a task whose status is tracked with ZooKeeper',
             **kwargs):
    parser.add_argument('-a', '--app_name', help=help, **kwargs)
