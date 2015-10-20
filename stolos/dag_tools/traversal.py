from collections import defaultdict
import networkx as nx

from stolos.util import crossproduct, flatmap_with_kwargs

from stolos.exceptions import (
    _log_raise, _log_raise_if, DAGMisconfigured, InvalidJobId)

from stolos import configuration_backend as cb
from stolos import get_NS

from .build import build_dag
from .node import (parse_job_id, get_job_id_template, get_valid_job_id_values)
from . import log


def topological_sort(lst):
    """Given a list of (app_name, job_id) pairs,
    topological sort by the app_names

    This is useful for sorting the parents and children of a node if the node
    has complex dependencies
    """
    dct = defaultdict(list)
    for app_job in lst:
        dct[app_job[0]].append(app_job)
    for node in nx.topological_sort(build_dag()):
        for app_job2 in dct[node]:
            yield app_job2


def get_parents(app_name, job_id, include_dependency_group=False,
                filter_deps=(), _filter_parents=()):
    """Return an iterator over all parent (app_name, job_id) pairs
    Given a child app_name and job_id

    `include_dependency_group` - (bool) If True,
        yield (app_name, job_id, dependency_group_name) tuples instead
    `filter_deps` - (list|tuple) only yield parents from a particular
        dependency group

    """
    build_dag()  # run validations
    if job_id:
        parsed_job_id = parse_job_id(app_name, job_id)
        filter_deps = set(filter_deps)
        if 'dependency_group_name' in parsed_job_id:
            filter_deps.add(parsed_job_id['dependency_group_name'], )
    else:
        parsed_job_id = None

    ld = dict(  # log details
        app_name=app_name, job_id=job_id)
    for group_name, dep_group in _get_grps(app_name, filter_deps, ld):
        if not dep_group_and_job_id_compatible(dep_group, parsed_job_id):
            log.debug(
                'ignoring dependency group in call to get_parents',
                extra=dict(dependency_group_name=group_name, **ld))
            continue

        kwargs = dict(
            group_name=group_name, app_name=app_name, job_id=job_id, ld=ld,
            include_dependency_group=include_dependency_group
        )
        if isinstance(dep_group, cb.TasksConfigBaseSequence):
            gen = _get_parents_handle_subgroups(
                group_name, dep_group, _filter_parents, ld, kwargs)
            for rv in gen:
                yield rv
        else:
            _get_parents_validate_app_name(
                app_names=dep_group['app_name'], ld=ld,
                _filter_parents=_filter_parents)
            for rv in _get_parents(dep_group=dep_group,
                                   _filter_parents=_filter_parents,
                                   **kwargs):
                yield rv


def dep_group_and_job_id_compatible(dep_group, pjob_id):
    """Check if the dependency group for this app could possibly have
    generated this job_id.  If it could have, then this dependency group
    contains parents and is compatible
    """
    if isinstance(dep_group, cb.TasksConfigBaseSequence):  # recursive AND
        return all(dep_group_and_job_id_compatible(dg, pjob_id)
                   for dg in dep_group)

    if pjob_id is None:
        return True  # all dependency groups are compatible

    for parent in dep_group['app_name']:
        _, parent_template = get_job_id_template(parent)

        for k, v in pjob_id.items():
            if k not in dep_group and k not in parent_template:
                return False
            if k in dep_group and v not in dep_group[k]:
                return False
    return True


def _get_parents_handle_subgroups(
        group_name, dep_group, _filter_parents, ld, kwargs):

    if _filter_parents:
        # validate filtered parents are a subset of parents
        all_app_names = set(
            y for x in dep_group for y in x['app_name'])
        _get_parents_validate_app_name(
            app_names=all_app_names, ld=ld,
            _filter_parents=_filter_parents)

    for subgrp in dep_group:
        if _filter_parents:
            app_names = set(_filter_parents).intersection(subgrp['app_name'])
            if not app_names:
                continue
        else:
            app_names = subgrp['app_name']
        for rv in _get_parents(
                dep_group=subgrp, _filter_parents=app_names, **kwargs):
            yield rv


def _get_grps(app_name, filter_deps, ld):
    """
    Return an iterator that yields (dependency_group_name, group_metadata)
    tuples
    """
    td = cb.get_tasks_config()
    try:
        depends_on = td[app_name]['depends_on']
    except KeyError:
        return []  # this task has no dependencies
    if "app_name" in depends_on:
        grps = [(get_NS().dependency_group_default_name, depends_on)]
        _get_parents_validate_group_names(
            [get_NS().dependency_group_default_name], filter_deps, ld)
    elif filter_deps:
        _get_parents_validate_group_names(
            depends_on, filter_deps, ld)
        grps = (data for data in depends_on.items()
                if data[0] in filter_deps)
    else:
        grps = depends_on.items()
    return grps


def _get_parents_validate_group_names(
        dep_names, filter_deps, ld):
    _log_raise_if(
        not set(dep_names).issuperset(filter_deps),
        "You specified dependency group names that don't exist",
        extra=dict(filter_deps=filter_deps, **ld),
        exception_kls=DAGMisconfigured)


def _get_parents_validate_app_name(app_names, ld, _filter_parents):
    """
    Ensure that we aren't trying to filter parent app_names with values
    that cannot exist in a particular dependency group
    """
    t = _filter_parents
    _log_raise_if(
        not set(app_names).issuperset(t),
        ("Misconfigured code.  You identified parents"
         " to a child that aren't this child's parents!"),
        extra=dict(
            known_parents=str(app_names),
            requested_parents=str(t),
            **ld),
        exception_kls=DAGMisconfigured)


def _get_parents(group_name, dep_group, app_name, job_id, ld,
                 include_dependency_group,
                 _filter_parents):
    """
    Handle some optional kwargs to get_parents.

    We're given a dependency group or a dependency subgroup as a dict.
    Safely modify the dict so it only contains relevant query terms defined
    by the `_filter_parents` option
    """
    if _filter_parents:
        dep_group = dict(dep_group)  # shallow copy to change the keys
        dep_group['app_name'] = _filter_parents

    for rv in _get_parent_job_ids(
            group_name, dep_group,
            child_app_name=app_name, child_job_id=job_id, ld=ld):
        if include_dependency_group:
            yield rv + (group_name, )
        else:
            yield rv


def _get_parent_job_ids(group_name, dep_group,
                        child_app_name, child_job_id, ld):
    """
    Yield the parent app_name and derived job_id for each parent listed in
    dep_group metadata

    If there is extra job_id criteria that doesn't apply to a
    particular parent app's job_id template, ignore it.
    """
    for parent_app_name in dep_group['app_name']:
        dep_group = dict(dep_group)  # shallow copy to change the keys

        _inject_job_id(
            dep_group, child_app_name, child_job_id, parent_app_name, ld)
        # are there specific job_ids the child would inherit from?
        if 'job_id' in dep_group:
            for rv in _iter_job_ids(dep_group=dep_group, group_name=group_name,
                                    parent_app_name=parent_app_name, ld=ld):
                yield rv
        else:
            # try to fill in the parent's job_id template and yield it
            template, parsed_template = get_job_id_template(parent_app_name)
            so_far = set()
            for job_id_data in crossproduct([dep_group[_key]
                                            for _key in parsed_template]):
                _pjob_id = dict(zip(parsed_template, job_id_data))
                parent_job_id = template.format(
                    dependency_group_name=group_name, **_pjob_id)
                if parent_job_id not in so_far:
                    so_far.add(parent_job_id)
                    yield (parent_app_name, parent_job_id)


def _inject_job_id(dep_group, child_app_name, child_job_id,
                   parent_app_name, ld):
    """Given metadata about a dependency group, set the dep_group['job_id']
    value.  Assume the dependency group only specifies an app_name key.
    Also, if the field for each identifier in the current job_id does
    not exist in the dependency group, add it.

    Basically, just update the dependency group with information """
    # if only "app_name" is defined in this dependency group,
    # assume child inherited the parent's job_id and passed that
    # to this child
    if child_job_id is None:
        _log_raise(
            ("It's impossible to get all parent job_ids if the"
                " child expects to inherit the parent's job_id and you"
                " haven't specified the child's job_id"),
            extra=dict(parent_app_name=parent_app_name, **ld),
            exception_kls=DAGMisconfigured)
    pjob_id = parse_job_id(child_app_name, child_job_id)
    if len(dep_group) == 1:
        t, pt = get_job_id_template(parent_app_name)
        try:
            dep_group['job_id'] = [t.format(**pjob_id)]
        except Exception as err:
            _log_raise(
                ("The child job_id doesn't contain enough pjob_id data to"
                 " create the parent job_id. Err details: %s") % err,
                extra=dict(job_id_template=t, pjob_iddata=str(pjob_id), **ld),
                exception_kls=err.__class__)
    else:
        for k, v in pjob_id.items():
            if k not in dep_group:
                dep_group[k] = [v]


def _iter_job_ids(dep_group, group_name, parent_app_name, ld):
    """
    Assume there specific job_ids listed in dependency group metadata that
    the child would inherit from and yield those.
    """
    for jid in dep_group['job_id']:
        try:
            parse_job_id(parent_app_name, jid)
        except InvalidJobId:
            _ld = dict(**ld)
            _ld.update(
                dependency_group_name=group_name,
                job_id=jid)
            _log_raise(
                ("There's no way parent could have the child's job_id"),
                extra=_ld,
                exception_kls=InvalidJobId)
        yield (parent_app_name, jid)


def get_children(app_name, job_id, include_dependency_group=True):
    dg = build_dag()
    child_apps = [(k, vv) for k, v in dg.succ[app_name].items() for vv in v]
    for child, group_name in child_apps:
        depends_on = dg.node[child]['depends_on']
        # 2 types of depends_on definitions:
        # 1) dict with app_name
        # 2) named dependency groups:
        #     2a) dict without app_name that defines a list of dicts (AND)
        #     2b) dict without app_name that defines a single dict (OR)
        if group_name != get_NS().dependency_group_default_name:
            depends_on = depends_on[group_name]

        kwargs = dict(
            func=_generate_job_ids,
            kwarg_name='depends_on', list_or_value=depends_on,
            app_name=app_name, job_id=job_id, child=child, group_name=group_name)
        for rv in flatmap_with_kwargs(**kwargs):
            if include_dependency_group:
                yield rv + (group_name, )
            else:
                yield rv


def _generate_job_ids(app_name, job_id, child, group_name, depends_on):
    # ignore dependency groups that have nothing to do with the parent app_name
    if app_name not in depends_on['app_name']:
        return []

    # if len(depends_on) == 1:
        # # child depends only on one parent, so it must be the parent we've
        # # called get_children on!
        # return [(child, job_id)]

    # check that the job_id applies to this group
    pjob_id = parse_job_id(app_name, job_id)  # parent data
    ctemplate, cparsed_template = get_job_id_template(child)  # child data

    # check if parent job_ids are hardcoded into configuration
    if 'job_id' in depends_on:
        if job_id in depends_on['job_id']:
            kwargs = dict()
            kwargs.update(pjob_id)
            kwargs.update({k: v[0] for k, v in depends_on.items()
                           if len(v) == 1})
            cjob_id = ctemplate.format(**kwargs)
            return [(child, cjob_id)]
        return []
    # check if the parent job_id template is compatible with this dep_grp
    child_valid_job_id_vals = get_valid_job_id_values(child, raise_err=False)
    for k, v in pjob_id.items():
        # is the parent's job_id identifier defined anywhere?
        if k not in depends_on and k not in cparsed_template:
            return []
        # is the identifier appropriately missing from the dep_grp?
        if k in depends_on and v not in depends_on[k]:
            return []
        # is parent identifier defined in child valid_job_id_values different
        # than parent's given job id?

        if k in child_valid_job_id_vals and v not in child_valid_job_id_vals[k]:
            return []

    # check that child's valid_job_id_values are defined if parent doesn't
    # completely define a child's job_id components.
    required_valid_job_id_values = set(cparsed_template).difference(pjob_id)
    _log_raise_if(
        any(x not in child_valid_job_id_vals
            for x in required_valid_job_id_values),
        "valid_job_id_values must be defined on child app_name if you have a"
        " parent whose job_id template is not a superset of the child's",
        extra=dict(
            child_app_name=child, parent_app_name=app_name,
            required_valid_job_id_values=required_valid_job_id_values),
        exception_kls=DAGMisconfigured)

    # check if the child's job_id template is compatible with this dep_grp
    for k in cparsed_template:
        # is child's job_id identifier appropriately missing from the dep_grp?
        if k in depends_on and k in pjob_id and pjob_id[k] not in depends_on[k]:
            return []
        # is identifier defined anywhere?
        if (
                k not in depends_on and
                k not in pjob_id and
                k not in get_valid_job_id_values(child, raise_err=False)
                ):
            return []
    return _generate_job_ids2(
        depends_on, pjob_id, cparsed_template, ctemplate, group_name, child)


def _generate_job_ids2(depends_on, pjob_id,
                       cparsed_template, ctemplate, group_name, child):
    so_far = set()
    valid_job_id_values = get_valid_job_id_values(child, raise_err=False)

    # Compile a list of lists, where each sublist contains the possible values
    # for a particular job_id component
    job_id_components = []
    for _key in cparsed_template:
        if _key in depends_on:
            job_id_components.append(depends_on[_key])
        elif _key in valid_job_id_values and \
                pjob_id.get(_key) not in valid_job_id_values[_key]:
            job_id_components.append(valid_job_id_values[_key].to_list())
        else:
            job_id_components.append([pjob_id[_key]])

    # Build job_ids using crossproduct of all components
    for job_id_data in crossproduct(job_id_components):
        cjob_id = ctemplate.format(
            dependency_group_name=group_name,
            **dict(zip(cparsed_template, job_id_data)))
        if cjob_id not in so_far:
            so_far.add(cjob_id)
            yield (child, cjob_id)
