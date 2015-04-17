"""
validations specific to zookeeper queue backend.
TODO: make these tests generic to any queue backend somehow
"""
from stolos import api
import nose.tools as nt
from os.path import join
from stolos import queue_backend as qb


def enqueue(app_name, job_id, validate_queued=True):
    # initialize job
    api.maybe_add_subtask(app_name, job_id)
    api.maybe_add_subtask(app_name, job_id)
    # verify initial conditions
    if validate_queued:
        validate_one_queued_task(app_name, job_id)


def cycle_queue(app_name):
    """Get item from queue, put at back of queue and return item"""
    q = api.get_qbclient().LockingQueue(app_name)
    item = q.get()
    q.put(item)
    q.consume()
    return item


def consume_queue(app_name):
    q = api.get_qbclient().LockingQueue(app_name)
    item = q.get(timeout=1)
    if item is not None:
        q.consume()
    return item


def get_qb_status(app_name, job_id):
    path = qb.get_job_path(app_name, job_id)
    elockpath = join(path, 'execute_lock')
    alockpath = join(path, 'add_lock')
    entriespath = join(app_name, 'entries')
    qbcli = api.get_qbclient()
    return {
        'num_add_locks': len(
            qbcli.exists(alockpath) and qbcli.get_children(alockpath) or []),
        'num_execute_locks': len(
            qbcli.exists(elockpath) and qbcli.get_children(elockpath) or []),
        'in_queue': (
            any(qbcli.get(join(app_name, 'entries', x)) == job_id
                for x in qbcli.get_children(entriespath))
            if qbcli.exists(entriespath) else False),
        'app_qsize': (
            len(qbcli.get_children(entriespath))
            if qbcli.exists(entriespath) else 0),
        'state': qbcli.get(path),
    }


def validate_zero_queued_task(app_name):
    qbcli = api.get_qbclient()
    if qbcli.exists(join(app_name, 'entries')):
        nt.assert_equal(
            0, len(qbcli.get_children(join(app_name, 'entries'))))


def validate_zero_completed_task(app_name):
    qbcli = api.get_qbclient()
    if qbcli.exists(join(app_name, 'all_subtasks')):
        nt.assert_equal(
            0, len(qbcli.get_children(join(app_name, 'all_subtasks'))))


def validate_one_failed_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    # nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'failed')


def validate_one_queued_executing_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 1)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], True)
    nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'pending')


def validate_one_queued_task(app_name, job_id):
    return validate_n_queued_task(app_name, job_id)


def validate_one_completed_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'completed')


def validate_one_skipped_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'skipped')


def validate_n_queued_task(app_name, *job_ids):
    for job_id in job_ids:
        status = get_qb_status(app_name, job_id)
        nt.assert_equal(status['num_execute_locks'], 0, job_id)
        nt.assert_equal(status['num_add_locks'], 0, job_id)
        nt.assert_equal(status['in_queue'], True, job_id)
        nt.assert_equal(status['app_qsize'], len(job_ids), job_id)
        nt.assert_equal(status['state'], 'pending', job_id)
