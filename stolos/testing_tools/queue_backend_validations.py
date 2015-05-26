"""
validations specific to zookeeper queue backend.
TODO: make these tests generic to any queue backend somehow
"""
from stolos import api
import nose.tools as nt
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
    item = q.get()
    if item is not None:
        q.consume()
    return item


def get_qb_status(app_name, job_id):
    path = qb.get_job_path(app_name, job_id)
    elockpath = qb.get_lock_path('execute', app_name, job_id)
    alockpath = qb.get_lock_path('add', app_name, job_id)
    qbcli = api.get_qbclient()
    return {
        'is_add_locked': qbcli.Lock(alockpath).is_locked(),
        'is_execute_locked': qbcli.Lock(elockpath).is_locked(),
        'in_queue': qbcli.LockingQueue(app_name).is_queued(job_id),
        'app_qsize': qbcli.LockingQueue(app_name).size(),
        'state': qbcli.get(path),
    }


def validate_not_exists(app_name, job_id):
    qbcli = api.get_qbclient()
    path = qb.get_job_path(app_name, job_id)
    nt.assert_false(qbcli.exists(path))


def validate_zero_queued_task(app_name):
    qbcli = api.get_qbclient()
    nt.assert_equal(qbcli.LockingQueue(app_name).size(), 0)


def validate_one_failed_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_false(status['is_execute_locked'])
    nt.assert_false(status['is_add_locked'])
    nt.assert_false(status['in_queue'])
    # nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'failed')


def validate_one_queued_executing_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_true(status['is_execute_locked'])
    nt.assert_false(status['is_add_locked'])
    nt.assert_true(status['in_queue'])
    nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'pending')


def validate_one_queued_task(app_name, job_id):
    return validate_n_queued_task(app_name, job_id)


def validate_one_completed_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_false(status['is_execute_locked'])
    nt.assert_false(status['is_add_locked'])
    nt.assert_false(status['in_queue'])
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'completed')


def validate_one_skipped_task(app_name, job_id):
    status = get_qb_status(app_name, job_id)
    nt.assert_false(status['is_execute_locked'])
    nt.assert_false(status['is_add_locked'])
    nt.assert_false(status['in_queue'])
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'skipped')


def validate_n_queued_task(app_name, *job_ids):
    for job_id in job_ids:
        status = get_qb_status(app_name, job_id)
        nt.assert_false(status['is_execute_locked'], job_id)
        nt.assert_false(status['is_add_locked'], job_id)
        nt.assert_true(status['in_queue'], job_id)
        nt.assert_equal(status['app_qsize'], len(job_ids), job_id)
        nt.assert_equal(status['state'], 'pending', job_id)
