import networkx as nx
import os
import tempfile

from scheduler import util
from scheduler.exceptions import _log_raise, _log_raise_if, DAGMisconfigured

from .constants import DEPENDENCY_GROUP_DEFAULT_NAME, JOB_ID_DEFAULT_TEMPLATE
from . import node


def _validate_dep_grp_metadata(dep_info, ld, tasks_dct, dep_name):
    ld1 = ld
    _template, child_template = node.get_job_id_template(ld['app_name'])
    for parent_app_name in dep_info['app_name']:
        ld = dict(
            parent_app_name=parent_app_name,
            dependency_group="depends_on.%s" % dep_name, **ld1)

        _log_raise_if(
            parent_app_name not in tasks_dct,
            "Unrecognized parent_app_name in a `depends_on` dependency group",
            extra=ld,
            exception_kls=DAGMisconfigured)
        if len(dep_info) == 1:
            _, parent_template = node.get_job_id_template(parent_app_name)
            _log_raise_if(
                not set(child_template).issuperset(parent_template),
                ("If you choose specify a dependency with no metadata, then"
                 " the"
                 " child task's job_id must be a superset of the metadata in"
                 " the parent's job_id. Otherwise, there"
                 " are cases where you cannot identify"
                 " a parent job_id given a child job_id."),
                extra=dict(
                    parent_job_id_template=parent_template,
                    child_job_id_template=child_template,
                    **ld),
                exception_kls=DAGMisconfigured)
    for k, v in dep_info.items():
        _log_raise_if(
            len(set(v)) != len(v),
            "You have duplicate metadata in dependency group metadata",
            extra=dict(key=k, value=v, **ld),
            exception_kls=DAGMisconfigured)

    for k in child_template:
        if len(dep_info) == 1 or 'job_id' in dep_info:
            break
        if k != 'dependency_group_name':
            _log_raise_if(
                k not in dep_info,
                ("The dependency group must specify all fields specified in"
                    " the job_id template"),
                extra=dict(key=k, job_id_template=_template, **ld),
                exception_kls=DAGMisconfigured)
            _log_raise_if(
                len(dep_info[k]) != 1,
                ("You cannot specify more than 1 value for each field"
                    " specified in your task's job_id template."),
                extra=dict(key=k, value=v, job_id_template=_template, **ld),
                exception_kls=DAGMisconfigured)


def _validate_dependency_groups_part2(dep_name, dep_info, ld, tasks_dct):
    _log_raise_if(
        ("app_name" not in dep_info
         or not isinstance(dep_info["app_name"], list)),
        ("Each dependency group the task depends on must specify"
         " an app_name key whose value is a list"),
        extra=dict(
            key="depends_on", invalid_dependency_group=dep_name,
            dep_info=str(dep_info), **ld),
        exception_kls=DAGMisconfigured)
    _validate_dep_grp_metadata(
        dep_info, ld=ld, tasks_dct=tasks_dct, dep_name=dep_name)


def _validate_dependency_groups(tasks_dct, metadata, ld):
    for dep_name, dep_info in metadata.get("depends_on", {}).items():
        _log_raise_if(
            dep_name in tasks_dct,
            ("Task's depends_on value has a naming conflict. You cannot"
             " identify a dependency group with the same name as an"
             " app_name."),
            extra=dict(
                key="depends_on",
                invalid_dependency_group=dep_name,
                **ld),
            exception_kls=DAGMisconfigured)
        if isinstance(dep_info, list):
            for _dep_info in dep_info:
                _validate_dependency_groups_part2(
                    dep_name, _dep_info, ld, tasks_dct)
            # check job_id template metadata is consistent
            # across the dependency group
            for identifier in node.get_job_id_template(ld['app_name'])[1]:
                values = [_dep_info.get(identifier) for _dep_info in dep_info
                          if 'job_id' not in _dep_info]
                _log_raise_if(
                    not reduce(lambda x, y: x == y, values),
                    ("You specified inconsistent values for job_id"
                     " metadata.  Each sub-dependency in your dependency"
                     " group must specify the exact same metadata value"
                     " for each identifier in your app's job_id template"),
                    extra=dict(
                        key="depends_on", invalid_dependency_group=dep_name,
                        invalid_identifier=identifier,
                        values=values, **ld),
                    exception_kls=DAGMisconfigured)
        else:
            _validate_dependency_groups_part2(
                dep_name, dep_info, ld, tasks_dct)


def validate_depends_on(app_name1, metadata, dg, tasks_dct, ld):
    _log_raise_if(
        not isinstance(metadata.get("depends_on", {}), dict),
        "Task's value at the depends_on key must be a dict",
        extra=dict(key="depends_on",
                   received_value_type=type(metadata.get('depends_on')),
                   **ld),
        exception_kls=DAGMisconfigured)
    # depends_on  - are we specifying only one unnamed dependency group?
    if "app_name" in metadata.get("depends_on", {}):
        _validate_dep_grp_metadata(
            metadata['depends_on'],
            ld=ld, tasks_dct=tasks_dct,
            dep_name=DEPENDENCY_GROUP_DEFAULT_NAME)
    # depends_on  - are we specifying specific dependency_groups?
    else:
        _validate_dependency_groups(tasks_dct, metadata, ld)
        # depends_on  -  are dependent tasks listed properly?
        for parent in dg.pred[app_name1]:
            _log_raise_if(
                parent not in tasks_dct,
                "Task defines an unrecognized parent dependency",
                extra=dict(parent_app_name=parent, **ld),
                exception_kls=DAGMisconfigured)


def validate_if_or(app_name1, metadata, dg, tasks_dct, ld):
    # valid_if_or  -  are we specifying what makes a job valid correctly?
    for k, v in metadata.get('valid_if_or', {}).items():
        assert isinstance(v, (list, set)), (
            "Task is misconfigured.  Expected a list but got"
            " %s. Location: %s"
            % (type(v), "%s.valid_if_or.%s" % (app_name1, k)))
        _log_raise_if(
            k not in metadata.get("job_id", JOB_ID_DEFAULT_TEMPLATE),
            "valid_if_or contains a key that isn't in its job_id template",
            extra=dict(
                key=k,
                job_id_template=metadata.get(
                    "job_id", JOB_ID_DEFAULT_TEMPLATE),
                **ld),
            exception_kls=DAGMisconfigured)
        for vv in v:
            assert isinstance(vv, (str, unicode)), (
                "Task is misconfigured.  Expected list of strings, but"
                " found a list with a %s element. Location: %s"
                % (type(vv), "%s.valid_if_or.%s" % (app_name1, k)))


def validate_job_type(app_name1, metadata, dg, tasks_dct, ld):
    # TODO: get these from plugins directory.

    # job_type  -  Is this a recognized job_type and does it exist?
    assert metadata.get('job_type') in ['pyspark', 'bash'], (
        "Invalid job_type for app_name: %s.  Job_type is: %s"
        % (app_name1, metadata.get('job_type', 'no job_type specified!')))


def validate_bash_opts(app_name1, metadata, dg, tasks_dct, ld):
    # bash_opts  -  Is job_type specified if has bash_opts?
    if metadata.get('bash_opts'):
        assert metadata.get('job_type') == 'bash', ((
            'Misconfigured task: %s.'
            ' If you specify bash_opts for a task, you must also set'
            ' job_type="bash"') % app_name1)
    assert isinstance(metadata.get('bash_opts', ''), (str, unicode))


def validate_spark_conf(app_name, metadata, dg, tasks_dct, ld):
    # spark_conf - Is it a dict of str: str pairs?
    _log_raise_if(
        not isinstance(metadata.get("spark_conf", {}), dict),
        "spark_conf, if supplied, must be a dict.",
        extra=dict(**ld),
        exception_kls=DAGMisconfigured
    )
    for k, v in metadata.get("spark_conf", {}).items():
        _log_raise_if(
            not isinstance(k, (unicode, str)),
            "Key in spark_conf must be a string",
            extra=dict(key=k, key_type=type(k), **ld),
            exception_kls=DAGMisconfigured)
        _log_raise_if(
            isinstance(v, (list, tuple, dict)),
            "Value for given key in spark_conf must be an int, string or bool",
            extra=dict(key=k, value_type=type(v), **ld),
            exception_kls=DAGMisconfigured)


def validate_env(app_name, metadata, dg, tasks_dct, ld):
    _log_raise_if(
        not isinstance(metadata.get("env", {}), dict),
        "env, if supplied, must be dict",
        extra=dict(**ld),
        exception_kls=DAGMisconfigured
    )
    # TODO: check for str: str pairs?
    _log_raise_if(
        not isinstance(metadata.get("env_from_os", []), list),
        "env_from_os, if supplied, must be list of environment variables",
        extra=dict(**ld),
        exception_kls=DAGMisconfigured
    )
    for key in metadata.get("env_from_os", []):
        _log_raise_if(
            not os.environ.get(key),
            "os environment key not found",
            extra=dict(key=key, **ld),
            exception_kls=DAGMisconfigured)


def validate_uris(app_name, metadata, dg, tasks_dct, ld):
    key = "uris"
    msg = ("%s, if supplied, must be a list of hadoop-compatible filepaths"
           % key)
    _log_raise_if(
        not isinstance(metadata.get(key, []), list),
        msg, extra=dict(**ld), exception_kls=DAGMisconfigured)
    _log_raise_if(
        not all(isinstance(x, (unicode, str))
                for x in metadata.get(key, ['a'])),
        msg, extra=dict(**ld), exception_kls=DAGMisconfigured)


def validate_dag(dg, tasks_dct):
    assert nx.algorithms.dag.is_directed_acyclic_graph(dg)

    for app_name1, metadata in tasks_dct.items():
        ld = dict(app_name=app_name1)
        validate_depends_on(app_name1, metadata, dg, tasks_dct, ld)
        validate_if_or(app_name1, metadata, dg, tasks_dct, ld)
        validate_job_type(app_name1, metadata, dg, tasks_dct, ld)
        validate_bash_opts(app_name1, metadata, dg, tasks_dct, ld)
        validate_spark_conf(app_name1, metadata, dg, tasks_dct, ld)
        validate_env(app_name1, metadata, dg, tasks_dct, ld)
        validate_uris(app_name1, metadata, dg, tasks_dct, ld)


def visualize_dag(dg=None, plot=True):
    """For interactive use"""
    if not dg:
        dg = build_dag()
    tmpf = tempfile.mkstemp(suffix='.dot')[1]
    try:
        nx.write_dot(dg, '{0}'.format(tmpf))
        if plot:
            os.popen(
                'dot {0} -Tpng > {0}.png ; open {0}.png ; sleep .5'
                .format(tmpf))
    finally:
        if os.path.exists(tmpf):
            os.remove(tmpf)
        if os.path.exists(tmpf + '.png'):
            os.remove(tmpf + '.png')


def _add_nodes(tasks_dct, dg):
    """Add nodes to a networkx graph"""
    for app_name, attr_dict in tasks_dct.items():
        dg.add_node(app_name, attr_dict)
        deps = attr_dict.get('depends_on')
        if deps:
            yield (app_name, deps)


def _build_dict_deps_part2(dg, app_name, dep_name, dep_info, log_details):
    """... add the freaking edge(s) already.

    `dg` is an instance of a nx.MultiDiGraph, which basically means we can have
    multiple edges between two nodes
    `dep_name` and `dep_info` are { key: value } of form:

       "dependency_name2": {
         "app_name": ["test_app/test_pyspark"],
         "date": [20140601],
         "client_id": [123, 140, 150],
         ...
       }
    """
    parent = dep_info['app_name']
    if isinstance(parent, (unicode, str)):
        dg.add_edge(parent, app_name, key=dep_name, label=dep_name)
    elif isinstance(parent, list):
        for _parent in parent:
            dg.add_edge(_parent, app_name, key=dep_name, label=dep_name)
    else:
        _log_raise(
            "Dependencies in DAG must specify an app_name"
            " or list of app_names",
            dict(parent_app_name=parent, **log_details),
            exception_kls=DAGMisconfigured)


def _build_dict_deps(dg, app_name, deps):
    """Build edges between dependent nodes, assuming those dependencies are
    defined in dict format."""
    log_details = dict(app_name=app_name, key='depends_on', deps=str(deps))
    if "app_name" in deps:
        _build_dict_deps_part2(
            dg, app_name=app_name, dep_name=DEPENDENCY_GROUP_DEFAULT_NAME,
            dep_info=deps, log_details=log_details)
    else:
        for dep_name, dep_info in deps.items():
            if isinstance(dep_info, dict):
                _build_dict_deps_part2(
                    dg=dg, app_name=app_name, dep_name=dep_name,
                    dep_info=dep_info, log_details=log_details)
            elif isinstance(dep_info, list):
                for _dep_info in dep_info:
                    _build_dict_deps_part2(
                        dg=dg, app_name=app_name, dep_name=dep_name,
                        dep_info=_dep_info, log_details=log_details)
            else:
                _log_raise(
                    "Unrecognized dependency.  Expected a list or dict",
                    dict(dep_name=dep_name, dep_info=dep_info, **log_details),
                    exception_kls=DAGMisconfigured)


@util.cached
def build_dag(tasks_dct=None):
    return _build_dag(tasks_dct)


def _build_dag(tasks_dct):
    if tasks_dct is None:
        tasks_dct = node.get_tasks_dct()
    dg = nx.MultiDiGraph()
    for app_name, deps in _add_nodes(tasks_dct, dg):
        _build_dict_deps(
            dg=dg, app_name=app_name, deps=deps)

    validate_dag(dg, tasks_dct)
    return dg
