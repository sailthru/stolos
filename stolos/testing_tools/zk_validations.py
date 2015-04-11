"""
validations specific to zookeeper queue backend.
TODO: make these tests generic to any queue backend somehow
"""
from stolos import api
import nose.tools as nt
from os.path import join
from stolos import zookeeper_tools as zkt


def enqueue(app_name, job_id, zk, validate_queued=True):
    # initialize job
    api.maybe_add_subtask(app_name, job_id, zk)
    api.maybe_add_subtask(app_name, job_id, zk)
    # verify initial conditions
    if validate_queued:
        validate_one_queued_task(zk, app_name, job_id)


def cycle_queue(zk, app_name):
    """Get item from queue, put at back of queue and return item"""
    q = zk.LockingQueue(app_name)
    item = q.get()
    q.put(item)
    q.consume()
    return item


def consume_queue(zk, app_name, timeout=1):
    q = zk.LockingQueue(app_name)
    item = q.get(timeout=timeout)
    q.consume()
    return item


def get_zk_status(zk, app_name, job_id):
    path = zkt._get_zookeeper_path(app_name, job_id)
    elockpath = join(path, 'execute_lock')
    alockpath = join(path, 'add_lock')
    entriespath = join(app_name, 'entries')
    return {
        'num_add_locks': len(
            zk.exists(alockpath) and zk.get_children(alockpath) or []),
        'num_execute_locks': len(
            zk.exists(elockpath) and zk.get_children(elockpath) or []),
        'in_queue': (
            any(zk.get(join(app_name, 'entries', x))[0] == job_id
                for x in zk.get_children(entriespath))
            if zk.exists(entriespath) else False),
        'app_qsize': (
            len(zk.get_children(entriespath))
            if zk.exists(entriespath) else 0),
        'state': zk.get(path)[0],
    }


def validate_zero_queued_task(zk, app_name):
    if zk.exists(join(app_name, 'entries')):
        nt.assert_equal(
            0, len(zk.get_children(join(app_name, 'entries'))))


def validate_zero_completed_task(zk, app_name):
    if zk.exists(join(app_name, 'all_subtasks')):
        nt.assert_equal(
            0, len(zk.get_children(join(app_name, 'all_subtasks'))))


def validate_one_failed_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    # nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'failed')


def validate_one_queued_executing_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 1)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], True)
    nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'pending')


def validate_one_queued_task(zk, app_name, job_id):
    return validate_n_queued_task(zk, app_name, job_id)


def validate_one_completed_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'completed')


def validate_one_skipped_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'skipped')


def validate_n_queued_task(zk, app_name, *job_ids):
    for job_id in job_ids:
        status = get_zk_status(zk, app_name, job_id)
        nt.assert_equal(status['num_execute_locks'], 0, job_id)
        nt.assert_equal(status['num_add_locks'], 0, job_id)
        nt.assert_equal(status['in_queue'], True, job_id)
        nt.assert_equal(status['app_qsize'], len(job_ids), job_id)
        nt.assert_equal(status['state'], 'pending', job_id)
