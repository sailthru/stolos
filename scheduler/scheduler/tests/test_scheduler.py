from contextlib import contextmanager
from os.path import join
import nose
import os
import subprocess
import tempfile
import ujson

from ds_commons.log import log
from scheduler import zookeeper_tools as zkt, exceptions, dag_tools as dt

CMD = (
    'python -m scheduler.runner --zookeeper_hosts localhost:2181'
    ' -a {app_name} {extra_opts}'
)
TASKS_JSON = os.environ['TASKS_JSON']
TASKS_JSON_TMPFILES = {}

zk = zkt.get_client('localhost:2181')
job_id1 = '20140606_1111_profile'
job_id2 = '20140606_2222_profile'
app_name1, app_name2, app_name3, app_name4 = [None] * 4


def create_tasks_json(fname_suffix='', inject={}, rename=False):
    """
    Create a new default tasks.json file
    This of this like a useful setup() operation before tests.

    `inject` - (dct) (re)define new tasks to add to the tasks graph
    mark this copy as the new default.
    `fname_suffix` - suffix in the file name of the new tasks.json file
    `rename` - if True, change the name all tasks to include the fname_suffix

    """
    tasks_dct = dt.get_tasks_dct()
    tasks_dct.update(inject)

    f = tempfile.mkstemp(prefix='tasks_json', suffix=fname_suffix)[1]
    frv = ujson.dumps(tasks_dct)
    if rename:
        renames = [(k, "%s__%s" % (k, fname_suffix))
                   for k in tasks_dct]
        for k, new_k in renames:
            frv = frv.replace(ujson.dumps(k), ujson.dumps(new_k))
    with open(f, 'w') as fout:
        fout.write(frv)

    os.environ['TASKS_JSON'] = f
    try:
        dt.build_dag.cache.clear()
    except AttributeError:
        pass
    return f


def setup_func(func_name):
    """Code that runs just before each test"""
    zk.delete('test_scheduler', recursive=True)

    # TODO: figure out how to make this ugly convenience hack
    # better.  maybe functions should initialize app_name1 = get_app_name(name)
    oapp_name1 = 'test_scheduler/test_module'
    oapp_name2 ='test_scheduler/test_module2'
    app_name1 = '%s__%s' % (oapp_name1, func_name)
    app_name2 = '%s__%s' % (oapp_name2, func_name)
    app_name3 = 'test_scheduler/test_depends_on__%s' % func_name
    app_name4 = 'test_scheduler/test_bashworker__%s' % func_name
    global app_name1, app_name2, app_name3, app_name4

    f = create_tasks_json(fname_suffix=func_name, rename=True)
    TASKS_JSON_TMPFILES[func_name] = f


def teardown_func(func_name):
    """Code that runs just after each test"""
    os.environ['TASKS_JSON'] = TASKS_JSON
    os.remove(TASKS_JSON_TMPFILES[func_name])


def with_setup(func):
    """A nose test-aware decorator that lets you specify
    setup and teardown functions that know the name of the function they
    perform setup and teardown for"""
    setup = lambda: setup_func(func.func_name)
    teardown = lambda: teardown_func(func.func_name)
    return nose.tools.with_setup(setup, teardown)(func)


@contextmanager
def _inject_into_dag(new_task_dct):
    """Update (add or replace) tasks in dag with new task config.
    This should reset any cacheing within the scheduler app,
    but it's not guaranteed"""
    f = create_tasks_json(inject=new_task_dct)

    # verify injection worked
    dg = dt.get_tasks_dct()
    dag = dt.build_dag()
    for k, v in new_task_dct.items():
        assert dg[k] == v, (
            "test code: _inject_into_dag didn't insert the new tasks?")
        assert dag.node[k] == v, (
            "test code: _inject_into_dag didn't reset the dag graph")

    yield
    os.remove(f)
    dt.build_dag.cache.clear()


@with_setup
def test_no_tasks():
    """
    The script shouldn't fail if it doesn't find any queued tasks
    """
    run_spark_code(app_name1)
    validate_zero_queued_task(app_name1)
    validate_zero_queued_task(app_name2)


@with_setup
def test_create_child_task_after_one_parent_completed():
    # if you modify the tasks.json file in the middle of processing the dag
    # modifications to the json file should be recognized

    # the child task should run if another parent completes
    # but otherwise should not run until it's manually queued

    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    validate_one_completed_task(app_name1, job_id1)

    injected_app = 'test_scheduler/test_module_ch'
    dct = {
        injected_app: {
            "job_type": "bash",
            "depends_on": {"app_name": [app_name1, app_name2],},
        },
    }
    with _inject_into_dag(dct):
        validate_zero_queued_task(injected_app)
        # unnecessary side effect: app_name1 queues app_name2...
        consume_queue(app_name2)
        zkt.set_state(app_name2, job_id1, zk=zk, completed=True)

        validate_one_completed_task(app_name2, job_id1)
        validate_one_queued_task(injected_app, job_id1)
        run_code(injected_app, '--bash echo 123')
        validate_one_completed_task(injected_app, job_id1)


@with_setup
def test_create_parent_task_after_child_completed():
    # if you modify the tasks.json file in the middle of processing the dag
    # modifications to the json file should be recognized appropriately

    # we do not re-schedule the child unless parent is completed

    zkt.set_state(app_name1, job_id1, zk=zk, completed=True)
    validate_one_completed_task(app_name1, job_id1)

    injected_app = 'test_scheduler/test_module_ch'
    child_injapp = 'test_scheduler/testX'
    dct = {
        injected_app: {
            "job_type": "bash",
        },
        child_injapp: {
            "job_type": "bash",
            "depends_on": {"app_name": [injected_app]}
        }
    }
    with _inject_into_dag(dct):
        validate_zero_queued_task(injected_app)
        zkt.set_state(injected_app, job_id1, zk=zk, completed=True)
        validate_one_completed_task(injected_app, job_id1)
        validate_one_queued_task(child_injapp, job_id1)


@with_setup
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

    enqueue(app_name1, job_id1)
    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)


@with_setup
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


@with_setup
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


@with_setup
def test_rerun_manual_task():
    enqueue(app_name1, job_id1)
    validate_one_queued_task(app_name1, job_id1)

    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        zkt.readd_subtask(app_name1, job_id1, zk=zk)

    # test2: re-add function can't add tasks if they've never been added
    zk.delete('test_scheduler', recursive=True)
    zkt.readd_subtask(app_name1, job_id1, zk=zk)
    validate_one_queued_task(app_name1, job_id1)


@with_setup
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


@with_setup
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


@with_setup
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


@with_setup
def test_complex_dependencies_pull_push():
    job_id = '20140601_1'
    enqueue(app_name3, job_id)
    run_code(app_name3, '--bash echo 123')

    parents = dt.get_parents(app_name3, job_id)
    parents = list(dt.topological_sort(parents))
    for parent, pjob_id in parents[:-1]:
        zkt.set_state(parent, pjob_id, zk=zk, completed=True)
        validate_zero_queued_task(app_name3)
    zkt.set_state(*parents[-1], zk=zk, completed=True)
    validate_one_queued_task(app_name3, job_id)
    run_code(app_name3, '--bash echo 123')
    validate_one_completed_task(app_name3, job_id)


@with_setup
def test_complex_dependencies_readd():
    job_id = '20140601_1'

    # mark everything completed
    parents = list(dt.topological_sort(dt.get_parents(app_name3, job_id)))
    for parent, pjob_id in parents:
        zkt.set_state(parent, pjob_id, zk=zk, completed=True)
    # --> parents should queue our app
    validate_one_queued_task(app_name3, job_id)
    consume_queue(app_name3)
    zkt.set_state(app_name3, job_id, zk=zk, completed=True)
    validate_one_completed_task(app_name3, job_id)

    log.warn("OK... Now try complex dependency test with a readd")
    # re-complete the very first parent.
    # we assume that this parent is a root task
    parent, pjob_id = parents[0]
    zkt.readd_subtask(parent, pjob_id, zk=zk)
    validate_one_queued_task(parent, pjob_id)
    validate_zero_queued_task(app_name3)
    consume_queue(parent)
    zkt.set_state(parent, pjob_id, zk=zk, completed=True)
    validate_one_completed_task(parent, pjob_id)
    # since that parent re-queues children that may be app_name3's
    # parents, complete those too!
    for p2, pjob2 in dt.get_children(parent, pjob_id, False):
        if p2 == app_name3:
            continue
        consume_queue(p2)
        zkt.set_state(p2, pjob2, zk=zk, completed=True)
    # now, that last parent should have queued our application
    validate_one_queued_task(app_name3, job_id)
    run_code(app_name3, '--bash echo 123')
    validate_one_completed_task(app_name3, job_id)
    # phew!


@with_setup
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


@with_setup
def test_retry_failed_task():
    """
    Retry failed tasks up to max num retries and then remove self from queue

    Tasks should maintain proper task state throughout.
    """
    # create 2 tasks in same queue
    enqueue(app_name1, job_id1)
    enqueue(app_name1, job_id2, validate_queued=False)
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


@with_setup
def test_invalid_task():
    """Invalid tasks should be automatically completed"""
    job_id = '20140606_3333_content'
    enqueue(app_name2, job_id, validate_queued=False)
    validate_one_completed_task(app_name2, job_id)


@with_setup
def test_valid_task():
    """Valid tasks should be automatically completed"""
    job_id = '20140606_3333_profile'
    enqueue(app_name2, job_id)


@with_setup
def test_bashworker():
    """a bash task should execute properly """
    # queue task
    enqueue(app_name4, job_id1)
    validate_one_queued_task(app_name4, job_id1)
    # run failing task
    run_code(app_name4, '--bash thiscommandshouldfail')
    validate_one_queued_task(app_name4, job_id1)
    # run successful task
    run_code(app_name4, '--bash echo 123')
    validate_zero_queued_task(app_name4)


@with_setup
def test_app_has_default_params():
    enqueue(app_name1, job_id1)
    msg = 'output: %s'
    # Test default params exist
    _, logoutput = run_spark_code(
        app_name1, capture=True, raise_on_err=False)
    nose.tools.assert_in(
        "s3_read_key='fake_read_fp'", logoutput, msg % logoutput)
    nose.tools.assert_in("read_fp=''", logoutput, msg % logoutput)


@with_setup
def test_app_has_command_line_params():
    enqueue(app_name1, job_id1)
    msg = 'output: %s'
    # Test passed in params exist
    _, logoutput = run_spark_code(
        app_name1, extra_opts='--read_fp newfakereadfp',
        capture=True, raise_on_err=False)
    nose.tools.assert_in(
        'newfakereadfp', logoutput, msg % logoutput)


@with_setup
def test_failing_task():
    with nose.tools.assert_raises(Exception):
        run_spark_code(app_name1, '--jaikahhaha')


def enqueue(app_name, job_id, validate_queued=True):
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
