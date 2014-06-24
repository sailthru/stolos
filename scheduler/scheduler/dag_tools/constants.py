"""
Some hardcoded values and functions specific to our use of the tool
"""
import os
import importlib


DEPENDENCY_GROUP_DEFAULT_NAME = 'default'
JOB_ID_DEFAULT_TEMPLATE = os.environ['JOB_ID_DEFAULT_TEMPLATE']
JOB_ID_DELIMITER = os.environ.get('JOB_ID_DELIMITER', '_')
TASKS_JSON = os.environ['TASKS_JSON']

# __job_id_validations = os.environ['JOB_ID_VALIDATIONS'].rsplit('.', 1)
JOB_ID_VALIDATIONS = importlib.import_module(
    os.environ['JOB_ID_VALIDATIONS']
).JOB_ID_VALIDATIONS
