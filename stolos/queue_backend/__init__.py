from stolos import argparse_shared as at

import logging
log = logging.getLogger('stolos.queue_backend')


build_arg_parser = at.build_arg_parser([
    at.add_argument(
        '--zookeeper_hosts', help="The address to your Zookeeper cluster")
], description=(
    "Options that specify which queue to use to store state about your jobs")
)
