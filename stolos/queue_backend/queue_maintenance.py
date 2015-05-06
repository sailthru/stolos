from __future__ import unicode_literals
from os.path import join
import re
import sys

from stolos import exceptions
from . import shared, log
from .modify_job_state import readd_subtask
from .read_job_state import check_state


def get_qsize(app_name, queued=True, taken=True):
    """Get the number of objects in the queue"""
    return shared.get_qbclient().LockingQueue(app_name).size(
        queued=queued, taken=taken)


def delete(app_name, job_id, confirm=True,
           delete_from_queue=True, delete_job_state=True, dryrun=False):
    """Delete data for one or more job_ids.

    In general, this operation is unsafe.
    Deleting nodes that another process is currently reading from or otherwise
    aware of has unknown effects.  If you use this, you should to guarantee that
    no other processes are processing the job_id (this includes child and parent
    jobs).

    Fail if trying to delete a queued job

    `job_id` (str) either a single job_id or a list of them.
    `delete_job_state` (bool) if True, will attempt to remove knowledge of
        the node's state, including locks and retry count
    `dryrun` (bool) don't actually delete nodes

    """
    qbcli = shared.get_qbclient()
    if isinstance(job_id, (str, unicode)):
        job_id = set([job_id])
    if any(qbcli.LockingQueue(app_name).is_queued(j) for j in job_id if j):
        raise exceptions.JobAlreadyQueued("Cannot delete a queued job")
    paths_to_delete = [shared.get_job_path(app_name, j) for j in job_id if j]
    if confirm:
        log.info(
            "About to delete n nodes from queue backend", extra=dict(
                n=len(paths_to_delete), first_100_nodes=paths_to_delete[:100]))
        _promptconfirm("Permanently delete nodes?")
    rvs = {}
    for path in paths_to_delete:
        if dryrun:
            log.info(
                '(dryrun) delete node from queue backend',
                extra=dict(node=path))
        else:
            log.info('delete node from queue backend', extra=dict(node=path))
            rv = qbcli.delete(path, recursive=True)
            if rv is None:
                rvs[path] = 'deleted'
            elif rv is True:
                rvs[path] = 'did not exist'
            else:
                rvs[path] = 'not deleted'
    return rvs


def get_job_ids_by_status(app_name, regexp=None, **job_states):
    """
    Return a list of job_ids that match a given state.

    This will load all job_ids for a specific app_name, so you should consider
    stolos.api.check_state(...) first if you have a list of specific
    job_ids you care about.

    If `app_name` does not exist, just log a warning and return nothing.

    `job_states` (kws) keys: pending|failed|completed|skipped|all
                       vals: True|False
        If no job states are defined, assume all=True
        ie.  get_job_ids_by_status(app_name, pending=True, failed=True)
    """
    qbcli = shared.get_qbclient()
    if not job_states:
        job_states = dict(all=True)  # assume all job states
    path = shared.get_job_path(app_name, '')
    try:
        children = qbcli.get_children(path)
    except exceptions.NoNodeError:
        log.warn("Unrecognized app_name", extra=dict(app_name=app_name))
        return []
    job_ids = []
    for job_id in children:
        if regexp and not re.search(regexp, job_id):
            continue
        job_ids.append(job_id)

    if not job_ids:
        log.warn('no job_ids found matching regex and app_name', extra=dict(
            app_name=app_name, regexp=regexp))

    gen = zip(job_ids, check_state(app_name, job_ids, **job_states))
    return [job_id for job_id, inset in gen if inset is True]


def requeue(app_name, regexp=None, confirm=True, **job_states):
    """
    Get and requeue jobs for app_name where given job_states are True

    `job_states` (kws) keys: pending|failed|completed|skipped|all
                       vals: True|False
        ie.  requeue(app_name, pending=True, failed=True)
    """
    IDS = get_job_ids_by_status(app_name, regexp=regexp, **job_states)
    log.info(
        "A list of some of the failed job_ids for %s: \n%s"
        % (app_name, IDS[:200]))
    msg = "Re-add %s jobs for %s with these states: %s." % (
        len(IDS), app_name, ' and '.join(job_states))
    if confirm:
        _promptconfirm("%s  Ok?" % msg)
    else:
        log.info(msg)
    for job_id in IDS:
        readd_subtask(app_name, job_id)


def _promptconfirm(msg):
    """Interactively prompt user and require a Y/N response.
    Only useful in an interactive console"""
    rv = None
    while rv not in ['Y', 'yes']:
        rv = raw_input("%s [Y/N]" % msg)
        if rv in ['n', 'N', 'no']:
            sys.exit(1)
