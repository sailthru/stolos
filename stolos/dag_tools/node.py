"""
A collections of functions for extracting information from nodes in the graph
Assume a node == info about a task
"""
import importlib
import re

from stolos.exceptions import _log_raise, DAGMisconfigured, InvalidJobId
from stolos.util import load_obj_from_path

from .constants import (
    JOB_ID_DEFAULT_TEMPLATE, JOB_ID_VALIDATIONS, JOB_ID_DELIMITER,
    CONFIGURATION_BACKEND)
from . import log


def get_tasks_config():
    try:
        cb = load_obj_from_path(
            CONFIGURATION_BACKEND, dict(key='CONFIGURATION_BACKEND'))()
    except:
        log.error("Could not load configuration backend")
        raise
    return cb


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
        20140506_876_profile -->

        {'date': 20140506, 'client_id': 876, 'collection_name': 'profile'}

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
            assert val is not None, "validation func returned None"
            assert val is not False, "validation func returned False"
        except KeyError:
            val = _val
            log.warn(
                "No job_id validation for key.  You should implement one",
                extra=dict(job_id_key=key, **ld))
        except Exception as err:
            val = _val
            msg = "An identifier in a job_id failed validation"
            log.exception(msg, extra=dict(
                job_id_identifier=key, bad_value=_val, error_details=err,
                **ld))
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
    dg = get_tasks_config()
    meta = dg[app_name]
    ld = dict(app_name=app_name, job_id=job_id)
    try:
        dct = dict(meta['valid_if_or'])
    except (KeyError, TypeError):
        return True  # everything is valid

    if '_func' in dct:
        import_path = dct.pop('_func')  # safe because config is immutable
        try:
            func = load_obj_from_path(import_path, ld)
        except Exception as err:
            raise err.__class__(
                "valid_if_or._func misconfigured: %s" % err.message)

        if func(app_name, **pjob_id):
            return True

    for k, v in dct.items():
        try:
            kk = pjob_id[k]
        except KeyError:
            _log_raise(
                "valid_if_or contains a key that's not in the job_id",
                extra=dict(valid_if_or_key=k, **ld),
                exception_kls=DAGMisconfigured)
        vals = [JOB_ID_VALIDATIONS[k](x) for x in v]
        if kk in vals:
            return True
    return False


def get_pymodule(app_name):
    dg = get_tasks_config()
    module_name = dg[app_name]['pymodule']
    return importlib.import_module(module_name)


def get_job_id_template(app_name, template=JOB_ID_DEFAULT_TEMPLATE):
    dg = get_tasks_config()
    template = dg[app_name].get('job_id', template)
    parsed_template = re.findall(r'{(.*?)}', template)
    return (template, parsed_template)


def get_job_type(app_name):
    """Lookup the job_type in tasks graph"""
    dg = get_tasks_config()
    return dg[app_name]['job_type']


def get_task_names():
    """Lookup the tasks in the tasks graph"""
    dg = get_tasks_config()
    return dg.keys()


def get_bash_opts(app_name):
    """Lookup the bash command-line options for a bash task
    If they don't exist, return empty string"""
    dg = get_tasks_config()
    meta = dg[app_name]
    job_type = meta['job_type']
    try:
        assert job_type == 'bash'
    except AssertionError:
        log.error(
            "App is not a bash job", extra=dict(
                app_name=app_name, job_type=job_type))
    return meta.get('bash_opts', '')
