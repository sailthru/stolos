"""
A collections of functions for extracting information from nodes in the graph
Assume a node == info about a task
"""
import importlib
import re
import ujson
import os

from scheduler.exceptions import _log_raise, DAGMisconfigured, InvalidJobId
from ds_commons.log import log

from .constants import (
    JOB_ID_DEFAULT_TEMPLATE, JOB_ID_VALIDATIONS, JOB_ID_DELIMITER,)


def get_tasks_dct(fp=None):
    if fp is None:
        fp = os.environ['TASKS_JSON']
    return ujson.load(open(fp))


def create_job_id(app_name, **job_id_identifiers):
    templ, ptempl = get_job_id_template(app_name)
    rv = _validate_job_id_identifiers(
        app_name, [job_id_identifiers[k] for k in ptempl])
    return templ.format(**rv)


def parse_job_id(app_name, job_id, delimiter=JOB_ID_DELIMITER):
    """Convert given `job_id` into a dict

    `app_name` (str) identifies a task
    `job_id` (str) identifies an instance of a task (ie a subtask)
    `validations` (dict) functions to ensure parts of the job_id are
                         properly typed
    `job_id_template` (str) identifies which validations to apply
    `delimiter` (str) value to split job_id into different components

    ie:
        20140506_450_profile -->

        {'date': 20140506, 'client_id': 450, 'collection_name': 'profile'}

    Returned values are cast into the appropriate type by the validations funcs

    """
    template, ptemplate = get_job_id_template(app_name)
    vals = job_id.split(delimiter, len(ptemplate) - 1)
    ld = dict(job_id=job_id, app_name=app_name, job_id_template=template)
    if len(vals) != len(ptemplate):
        _log_raise(
            ("Job_id isn't properly delimited.  You might have too few"
             " or too many underscores."),
            extra=ld, exception_kls=InvalidJobId)
    return _validate_job_id_identifiers(app_name, vals)


def _validate_job_id_identifiers(
        app_name, vals, validations=JOB_ID_VALIDATIONS, **_log_details):
    _, template = get_job_id_template(app_name)
    ld = dict(app_name=app_name, job_id_template=template)
    ld.update(_log_details)
    rv = {}
    for key, _val in zip(template, vals):
        # validate the job_id
        try:
            val = validations[key](_val)
            assert val, "Failed validation!"
        except KeyError:
            log.warn(
                "No job_id validation for key.  You should implement one",
                extra=dict(job_id_key=key, **ld))
        except Exception as err:
            msg = "Given job_id is improperly formatted."
            log.exception(msg, extra=dict(
                expected=key, received=_val, error_details=err, **ld))
            raise InvalidJobId("%s err: %s" % (msg, err))
        rv[key] = val
    return rv


def passes_filter(app_name, job_id):
    """Determine if this job matches certain criteria that state it is a
    valid job for this app_name.

    A partially out of scope for dag stuff, but important detail:
        Jobs that don't match the criteria should immediately be marked
        as completed
    """
    # for now, if we can parse it, it's valid
    pjob_id = parse_job_id(app_name, job_id)

    # does this job matches criteria that makes it executable? if so, we can't
    # autocomplete it
    dg = get_tasks_dct()
    meta = dg[app_name]
    try:
        dct = meta['valid_if_or']
    except (KeyError, TypeError):
        return True  # everything is valid

    for k, v in dct.items():
        try:
            kk = pjob_id[k]
        except KeyError:
            _log_raise(
                "valid_if_or contains a key that's not in the job_id",
                extra=dict(
                    app_name=app_name, job_id=job_id, valid_if_or_key=k),
                exception_kls=DAGMisconfigured)
        if kk in v:
            return True
    return False


def get_pymodule(app_name):
    dg = get_tasks_dct()
    module_name = dg[app_name]['pymodule']
    return importlib.import_module(module_name)


def get_job_id_template(app_name, template=JOB_ID_DEFAULT_TEMPLATE):
    dg = get_tasks_dct()
    template = dg[app_name].get('job_id', template)
    parsed_template = re.findall(r'{(.*?)}', template)
    return (template, parsed_template)


def get_job_type(app_name):
    """Lookup the job_type in tasks graph"""
    dg = get_tasks_dct()
    return dg[app_name]['job_type']


def get_task_names():
    """Lookup the tasks in the tasks graph"""
    dg = get_tasks_dct()
    return dg.keys()


def get_bash_opts(app_name):
    """Lookup the bash command-line options for a bash task
    If they don't exist, return empty string"""
    dg = get_tasks_dct()
    meta = dg[app_name]
    job_type = meta['job_type']
    try:
        assert job_type == 'bash'
    except AssertionError:
        log.error(
            "App is not a bash job", extra=dict(
                app_name=app_name, job_type=job_type))
    return meta.get('bash_opts', '')


def get_spark_conf(app_name):
    dg = get_tasks_dct()
    conf = dict(**dg[app_name].get('spark_conf', {}))
    conf['spark.app.name'] = app_name
    osenv = dg[app_name].get('env', {})
    pyFiles = dg[app_name].get('uris', [])
    files = []  # for now, we're ignoring files.
    return conf, osenv, files, pyFiles
