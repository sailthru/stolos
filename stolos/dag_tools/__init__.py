"""
This sub-package contains code to work with the tasks graph.  The configuration
data used by this package comes from a configuration backend.

This __init__ file is the official api for how other Stolos internals should
use this sub-package.  External projects should refer directly to Stolos.api.
"""
import importlib
from stolos import argparse_shared as at

import logging
log = logging.getLogger('stolos.dag_tools')


# define configuration dependencies for dag_tools to be usable
build_arg_parser = at.build_arg_parser([at.group(
    # "DAG: Details relating to how your app dependencies are defined",
    "Application Dependency Configuration",
    at.add_argument(
        '--job_id_default_template', required=True, help=(
            "Defines the default way to identify `job_id`s for all"
            " applications.  See conf/stolos-env.sh for an example")),
    at.add_argument(
        '--job_id_validations', required=True,
        type=lambda pth: importlib.import_module(pth).JOB_ID_VALIDATIONS,
        help=(
            'A python import path to a python module where Stolos can expect'
            ' to find the a dict named `JOB_ID_VALIDATIONS`.  This dict'
            ' contains validation functions for job_id components.'
            ' You can also configure Stolos logging here.'
            ' See conf/stolos-env.sh for an example')),
    at.add_argument(
        '--job_id_delimiter', default='_', help=(
            'The identifying components of a job_id (as defined in'
            ' the job_id_template) are separated by a character sequence.'
            ' The default for this is an underscore: "_"')),
    at.add_argument(
        "--dependency_group_default_name", default='default', help=(
            'A very low-level option that specifies how unnamed dependency'
            " groups are identified. Don't bother changing this")),
)])


# Expose various functions to the rest of Stolos internals
from .build import (
    build_dag,
    visualize_dag,
)
from .node import (
    create_job_id,
    parse_job_id,
    passes_filter,
    get_job_id_template,
    get_job_type,
    get_task_names,
)

from .traversal import (
    get_parents,
    get_children,
    topological_sort,
)
build_dag, visualize_dag
create_job_id, parse_job_id, passes_filter, get_job_id_template, get_job_type,
get_task_names
get_parents, get_children, topological_sort
