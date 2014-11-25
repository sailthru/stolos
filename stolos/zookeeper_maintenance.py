from __future__ import unicode_literals
from os.path import join, dirname
import re
import sys

from stolos import zookeeper_tools as zkt, log


def delete(app_name, job_id, zk, confirm=True, delete_from_queue=True):
    """Delete Zookeeper data for a specific job.  In general, this operation is
    unsafe.  Deleting nodes that another process is currently reading from or
    otherwise aware of has unknown effects"""
    path = zkt._get_zookeeper_path(app_name, job_id)
    path2 = None
    path2_job_id = None
    if delete_from_queue:
        ddir = join(dirname(dirname(path)), 'entries')
        queue = [
            (zk.get(join(ddir, node))[0], node)
            for node in zk.get_children(ddir)]
        _path2 = [
            (join(ddir, node), job_id2)
            for job_id2, node in queue
            if job_id2 == job_id]
        assert len(_path2) <= 1
        if _path2:
            path2 = _path2[0][0]
            path2_job_id = _path2[0][1]
    if confirm:
        promptconfirm(
            "Permanently delete node?  \n%s\n%s --> %s\n\n"
            "Are you sure you want to delete these nodes?" % (
                path, path2, path2_job_id))
    # Don't delete the root!
    if path:
        log.info("deleting node from zookeeper", extra=dict(node=path))
        zk.delete(path, recursive=True)
    if delete_from_queue and path2:
        log.info("deleting node from zookeeper", extra=dict(node=path2))
        zk.delete(path2, recursive=True)


def get_job_ids_by_status(app_name, zk, regexp=None, **job_states):
    """
    Return a list of job_ids that match a given state

    `job_states` (kws) keys: pending|failed|completed|skipped|all
                       vals: True|False
        ie.  get_job_ids_by_status(app_name, zk, pending=True, failed=True)
    """
    IDS = []
    path = zkt._get_zookeeper_path(app_name, '')
    for job_id in zk.get_children(path):
        if regexp and not re.search(regexp, job_id):
            continue
        if job_states.get('all'):  # basecase
            IDS.append(job_id)
            continue

        state = zkt.check_state(app_name, job_id, zk=zk, _get=True)
        if job_states.get('failed') and state == zkt.ZOOKEEPER_FAILED:
            IDS.append(job_id)
        if job_states.get('pending') and state == zkt.ZOOKEEPER_PENDING:
            IDS.append(job_id)
        if job_states.get('completed') and state == zkt.ZOOKEEPER_COMPLETED:
            IDS.append(job_id)
        if job_states.get('skipped') and state == zkt.ZOOKEEPER_SKIPPED:
            IDS.append(job_id)
    return IDS


def requeue(app_name, zk, regexp=None, **job_states):
    """
    Get and requeue jobs for app_name where given job_states are True

    `job_states` (kws) keys: pending|failed|completed|skipped|all
                       vals: True|False
        ie.  requeue(app_name, zk, pending=True, failed=True)
    """
    IDS = get_job_ids_by_status(app_name, zk, regexp=regexp, **job_states)
    log.info(
        "A list of some of the failed job_ids for %s: \n%s"
        % (app_name, IDS[:200]))
    promptconfirm(
        "Re-add %s jobs for %s with these states: %s. Ok?" %
        (len(IDS), app_name, ' and '.join(job_states)))
    for job_id in IDS:
        zkt.readd_subtask(app_name, job_id, zk=zk)


def requeue_failed(app_name, zk):
    """Requeue failed jobs for given app"""
    return requeue(app_name, zk, failed=True)


def get_state(app_name, zk):
    """Get the state of a job from Zookeeper.  This implies that you understand
    how Zookeeper states are named.

    It's a much better idea to call something like
        check_state(..., pending=True)
    """
    path = zkt._get_zookeeper_path(app_name, '')
    return {
        job_id: zkt.check_state(app_name, job_id, zk=zk, _get=True)
        for job_id in zk.get_children(path)}


def get_failed(app_name, zk):
    """Return a list of job_ids with status == failed"""
    path = zkt._get_zookeeper_path(app_name, '')
    return [
        job_id for job_id in zk.get_children(path)
        if zkt.check_state(app_name, job_id, zk=zk, failed=True)]


def promptconfirm(msg):
    """Interactively prompt user and require a Y/N response.
    Only useful in an interactive console"""
    rv = None
    while rv not in ['Y', 'yes']:
        rv = raw_input("%s [Y/N]" % msg)
        if rv in ['n', 'N', 'no']:
            sys.exit(1)
