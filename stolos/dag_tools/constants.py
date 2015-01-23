"""
Some hardcoded values and functions specific to our use of the tool
"""
import os
import importlib
from stolos import argparse_shared as at


build_arg_parser = at.build_arg_parser([at.group(
    "Options that specify how your apps are defined in the dependency graph",

DEPENDENCY_GROUP_DEFAULT_NAME = 'default'
JOB_ID_DEFAULT_TEMPLATE = os.environ['JOB_ID_DEFAULT_TEMPLATE']
JOB_ID_DELIMITER = os.environ.get('JOB_ID_DELIMITER', '_')

# __job_id_validations = os.environ['JOB_ID_VALIDATIONS'].rsplit('.', 1)
JOB_ID_VALIDATIONS = importlib.import_module(
    os.environ['JOB_ID_VALIDATIONS']
).JOB_ID_VALIDATIONS

CONFIGURATION_BACKEND = os.environ.get(
    'CONFIGURATION_BACKEND',
    'stolos.configuration_backend.json_config.JSONMapping')
