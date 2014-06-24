from os.path import join, basename
import nose
import subprocess

from ds_commons.log import log
from scheduler import zookeeper_tools as zkt, exceptions, dag_tools as dt

CMD = (
    'python -m scheduler.runner --zookeeper_hosts localhost:2181'
    ' -a {app_name} {extra_opts}'
)

zk = zkt.get_client('localhost:2181')
job_id1 = '20140606_1111_profile'
job_id2 = '20140606_2222_profile'
# TODO: remove this hardcoded value so we can parallelize tests
app_name1 = 'test_scheduler/test_module'
app_name2 = 'test_scheduler/test_module2'  # depends on app_name1 in tasks.json


def test_no_tasks():
    """
    The script shouldn't fail if it doesn't find any queued tasks
    """
    zk.delete(basename(app_name1), recursive=True)
    run_spark_code(app_name1)
    validate_zero_queued_task(app_name1)


def test_create_child_task_after_parents_completed():

    # TODO: should raise a warning (or error?) when child runs and finds its
    # score state doesn't reflect reality
    # should verify that ChildOutOfSync errors get raised in this case
    pass  # TODO


def test_should_not_add_queue_while_consuming_queue():
    """
    This test guards from doubly queuing jobs
    This protects from simultaneous operations on root and leaf nodes
    ie (parent and child) for the following operations:
    adding, readding or a mix of both
    """
    enqueue(app_name1, job_id1)

    q = zk.LockingQueue(app_name1)
    q.get()
    validate_one_queued_task(app_name1, job_id1)

    enqueue(app_name1, job_id1, delete=False)
    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)


def test_push_tasks():
    """
    Child tasks should be generated and executed properly

    if task A --> task B, and we queue & run A,
    then we should end up with one A task completed and one queued B task
    """
    enqueue(app_name1, job_id1)
    run_spark_code(app_name1)
    validate_one_completed_task(app_name1, job_id1)
    # check child
    validate_one_queued_task(app_name2, job_id1)


def test_rerun_pull_tasks():
    # queue and complete app 1. it queues a child
    enqueue(app_name1, job_id1)
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    validate_zero_queued_task(app_name1)
    validate_one_queued_task(app_name2, job_id1)
    # complete app 2
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_zero_queued_task(app_name2)

    # readd app 2
    zkt.readd_subtask(app_name2, job_id1, zk=zk)
    validate_zero_queued_task(app_name1)
    validate_one_queued_task(app_name2, job_id1)
    # run app 2.  the parent was previously completed
    run_spark_code(app_name2)
    validate_one_completed_task(app_name1, job_id1)  # previously completed
    validate_one_completed_task(app_name2, job_id1)


def test_rerun_manual_task():
    enqueue(app_name1, job_id1)
    validate_one_queued_task(app_name1, job_id1)

    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        zkt.readd_subtask(app_name1, job_id1, zk=zk)

    # test2: re-add function can't add tasks if they've never been added
    zk.delete('test_scheduler', recursive=True)
    zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)


def test_rerun_push_tasks_when_manually_queuing_child_and_parent():
    _test_rerun_tasks_when_manually_queuing_child_and_parent()

    # complete parent first
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)

    # child completes normally
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)


def test_rerun_pull_tasks_when_manually_queuing_child_and_parent():
    _test_rerun_tasks_when_manually_queuing_child_and_parent()

    # complete child first
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    # --> parent still queued
    validate_one_queued_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)

    # then complete parent
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    # --> child gets re-queued
    validate_one_completed_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)

    # complete child second time
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)


def _test_rerun_tasks_when_manually_queuing_child_and_parent():
    # complete parent and child
    enqueue(app_name1, job_id1)
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)

    # manually re-add child
    zkt.readd_subtask(app_name2, job_id1, zk=zk)
    validate_one_queued_task(app_name2, job_id1)
    validate_one_completed_task(app_name1, job_id1)

    # manually re-add parent
    zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)


def test_rerun_push_tasks():
    # this tests recursively deleteing parent status on child nodes

    # queue and complete app 1. it queues a child
    enqueue(app_name1, job_id1)
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)

    # complete app 2
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)

    # readd app 1
    zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)
    validate_zero_queued_task(app_name2)
    nose.tools.assert_true(
        zkt.check_state(app_name2, job_id1, zk=zk, pending=True))

    # complete app 1
    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    consume_queue(app_name1)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)
    # complete app 2
    zkt.set_state(app_name2, job_id1, zk=zk, completed=True)
    consume_queue(app_name2)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_completed_task(app_name2, job_id1)


def test_complex_dependencies_pull_push():
    app_name = 'test_scheduler/test_depends_on'
    job_id = '20140601_1'
    zk.delete('test_scheduler', recursive=True)
    enqueue(app_name, job_id)
    run_code(app_name, '--bash echo 123')

    parents = list(sorted(dt.get_parents(app_name, job_id)))
    for parent, pjob_id in parents[:-1]:
        print(parent, pjob_id)
        consume_queue(parent)
        zkt.set_state(parent, pjob_id, zk=zk, completed=True)
        validate_zero_queued_task(app_name)
    print(parents[-1])
    consume_queue(parents[-1][0])
    zkt.set_state(*parents[-1], zk=zk, completed=True)
    validate_one_queued_task(app_name, job_id)
    run_code(app_name, '--bash echo 123')
    validate_one_completed_task(app_name, job_id)


def test_complex_dependencies_readd():
    app_name = 'test_scheduler/test_depends_on'
    job_id = '20140601_1'
    zk.delete('test_scheduler', recursive=True)

    # mark everything completed
    parents = list(sorted(dt.get_parents(app_name, job_id)))
    for parent, pjob_id in parents:
        zkt.set_state(parent, pjob_id, zk=zk, completed=True)
    # --> parents should queue our app
    validate_one_queued_task(app_name, job_id)
    consume_queue(app_name)
    zkt.set_state(app_name, job_id, zk=zk, completed=True)
    validate_one_completed_task(app_name, job_id)

    log.warn("OK... Now try complex dependency test with a readd")
    # re-complete the very first parent.
    # we assume that this parent is a root task
    parent, pjob_id = parents[0]
    zkt.readd_subtask(parent, pjob_id, zk=zk)
    validate_one_queued_task(parent, pjob_id)
    validate_zero_queued_task(app_name)
    consume_queue(parent)
    zkt.set_state(parent, pjob_id, zk=zk, completed=True)
    validate_one_completed_task(parent, pjob_id)
    # since that parent re-queues children that may be test_depends_on's
    # parents, complete those too!
    for p2, pjob2 in dt.get_children(parent, pjob_id, False):
        if p2 == app_name:
            continue
        consume_queue(p2)
        zkt.set_state(p2, pjob2, zk=zk, completed=True)
    # now, that last parent should have queued our application
    validate_one_queued_task(app_name, job_id)
    run_code(app_name, '--bash echo 123')
    validate_one_completed_task(app_name, job_id)
    # phew!


def test_pull_tasks():
    """
    Parent tasks should be generated and executed before child tasks

    If A --> B, and:
        we queue and run B, then we should have 0 completed tasks,
        but A should be queued

        nothing should change until:
            we run A and A becomes completed
            we then run B and B becomes completed
    """
    enqueue(app_name2, job_id1)
    run_spark_code(app_name2)
    validate_one_queued_task(app_name1, job_id1)
    validate_zero_queued_task(app_name2)

    run_spark_code(app_name2)
    validate_one_queued_task(app_name1, job_id1)
    validate_zero_queued_task(app_name2)

    run_spark_code(app_name1)
    validate_one_completed_task(app_name1, job_id1)
    validate_one_queued_task(app_name2, job_id1)
    run_spark_code(app_name2)
    validate_one_completed_task(app_name2, job_id1)


def test_retry_failed_task():
    """
    Retry failed tasks up to max num retries and then remove self from queue

    Tasks should maintain proper task state throughout.
    """
    # create 2 tasks in same queue
    enqueue(app_name1, job_id1)
    enqueue(app_name1, job_id2, delete=False, validate_queued=False)
    nose.tools.assert_equal(2, get_zk_status(app_name1, job_id1)['app_qsize'])
    nose.tools.assert_equal(job_id1, cycle_queue(app_name1))
    # run job_id2 and have it fail
    run_spark_code(app_name1, extra_opts='--fail')
    # ensure we still have both items in the queue
    nose.tools.assert_true(get_zk_status(app_name1, job_id1)['in_queue'])
    nose.tools.assert_true(get_zk_status(app_name1, job_id2)['in_queue'])
    # ensure the failed task is sent to back of the queue
    nose.tools.assert_equal(2, get_zk_status(app_name1, job_id1)['app_qsize'])
    nose.tools.assert_equal(job_id1, cycle_queue(app_name1))
    # run and fail n times, where n = max failures
    run_spark_code(app_name1, extra_opts='--fail --max_retry 1')
    # verify that job_id2 is removed from queue
    validate_one_queued_task(app_name1, job_id1)
    # verify that job_id2 state is 'failed' and job_id1 is still pending
    validate_one_failed_task(app_name1, job_id2)


def test_invalid_task():
    """Invalid tasks should be automatically completed"""
    job_id = '20140606_3333_content'
    enqueue(app_name2, job_id, validate_queued=False)
    validate_one_completed_task(app_name2, job_id)


def test_valid_task():
    """Valid tasks should be automatically completed"""
    job_id = '20140606_3333_profile'
    enqueue(app_name2, job_id)


def test_bashworker():
    """a bash task should execute properly """
    app_name = 'test_scheduler/test_bashworker'
    # queue task
    enqueue(app_name, job_id1)
    validate_one_queued_task(app_name, job_id1)
    # run failing task
    run_code(app_name, '--bash thiscommandshouldfail')
    validate_one_queued_task(app_name, job_id1)
    # run successful task
    run_code(app_name, '--bash echo 123')
    validate_zero_queued_task(app_name)


def test_app_has_default_params():
    enqueue(app_name1, job_id1)
    msg = 'output: %s'
    # Test default params exist
    _, logoutput = run_spark_code(
        app_name1, capture=True, raise_on_err=False)
    nose.tools.assert_in(
        "s3_read_key='fake_read_fp'", logoutput, msg % logoutput)
    nose.tools.assert_in("read_fp=''", logoutput, msg % logoutput)


def test_app_has_command_line_params():
    enqueue(app_name1, job_id1)
    msg = 'output: %s'
    # Test passed in params exist
    _, logoutput = run_spark_code(
        app_name1, extra_opts='--read_fp newfakereadfp',
        capture=True, raise_on_err=False)
    nose.tools.assert_in(
        'newfakereadfp', logoutput, msg % logoutput)


def test_failing_task():
    with nose.tools.assert_raises(Exception):
        run_spark_code(app_name1, '--jaikahhaha')


def enqueue(app_name, job_id, delete=True, validate_queued=True):
    if delete:
        zk.delete('test_scheduler', recursive=True)

    # initialize job
    zkt.maybe_add_subtask(app_name, job_id, zk)
    zkt.maybe_add_subtask(app_name, job_id, zk)
    # verify initial conditions
    if validate_queued:
        validate_one_queued_task(app_name, job_id)


def run_spark_code(app_name, extra_opts='', **kwargs):
    extra_opts = ' --disable_log %s' % extra_opts
    return run_code(app_name, extra_opts, **kwargs)


def run_code(app_name, extra_opts, capture=False, raise_on_err=True):
    log.debug('run code', extra=dict(app_name=app_name, extra_opts=extra_opts))
    cmd = CMD.format(app_name=app_name, extra_opts=extra_opts)
    p = subprocess.Popen(
        cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    rc = p.poll()
    if rc:
        raise Exception(
            "command failed. returncode: %s\ncmd: %s\nstderr: %s\nstdout: %s\n"
            % (rc, cmd, stderr, stdout))
    if capture:
        return stdout, stderr


def validate_zero_queued_task(app_name):
    if zk.exists(join(app_name, 'entries')):
        nose.tools.assert_equal(
            0, len(zk.get_children(join(app_name, 'entries'))))


def validate_one_failed_task(app_name, job_id):
    status = get_zk_status(app_name, job_id)
    nose.tools.assert_equal(status['num_locks'], 0)
    nose.tools.assert_equal(status['in_queue'], False)
    # nose.tools.assert_equal(status['app_qsize'], 1)
    nose.tools.assert_equal(status['state'], 'failed')


def validate_one_queued_task(app_name, job_id):
    status = get_zk_status(app_name, job_id)
    nose.tools.assert_equal(status['num_locks'], 0)
    nose.tools.assert_equal(status['in_queue'], True)
    nose.tools.assert_equal(status['app_qsize'], 1)
    nose.tools.assert_equal(status['state'], 'pending')


def validate_one_completed_task(app_name, job_id):
    status = get_zk_status(app_name, job_id)
    nose.tools.assert_equal(status['num_locks'], 0)
    nose.tools.assert_equal(status['in_queue'], False)
    nose.tools.assert_equal(status['app_qsize'], 0)
    nose.tools.assert_equal(status['state'], 'completed')


def cycle_queue(app_name):
    """Get item from queue, put at back of queue and return item"""
    q = zk.LockingQueue(app_name)
    item = q.get()
    q.put(item)
    q.consume()
    return item


def consume_queue(app_name, timeout=1):
    q = zk.LockingQueue(app_name)
    item = q.get(timeout=timeout)
    q.consume()
    return item


def get_zk_status(app_name, job_id):
    path = zkt._get_zookeeper_path(app_name, job_id)
    lockpath = join(path, 'lock')
    entriespath = join(app_name, 'entries')
    return {
        'num_locks': len(zk.exists(lockpath)
                         and zk.get_children(lockpath) or []),
        'in_queue': (
            any(zk.get(join(app_name, 'entries', x))[0] == job_id
                for x in zk.get_children(entriespath))
            if zk.exists(entriespath) else False),
        'app_qsize': (
            len(zk.get_children(entriespath))
            if zk.exists(entriespath) else 0),
        'state': zk.get(path)[0],
    }
