from __future__ import unicode_literals
import sys

from stolos import exceptions
from . import shared, log


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


def _promptconfirm(msg):
    """Interactively prompt user and require a Y/N response.
    Only useful in an interactive console"""
    rv = None
    while rv not in ['Y', 'yes']:
        rv = raw_input("%s [Y/N]" % msg)
        if rv in ['n', 'N', 'no']:
            sys.exit(1)
