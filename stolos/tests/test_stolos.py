import nose
import subprocess

from stolos import api
from stolos import exceptions
from stolos import queue_backend as qb  # TODO: remove reliance on this
from stolos.testing_tools import (
    with_setup, inject_into_dag,
    enqueue, cycle_queue, consume_queue, get_qb_status,
    validate_zero_queued_task, validate_not_exists,
    validate_one_failed_task, validate_one_queued_executing_task,
    validate_one_queued_task, validate_one_completed_task,
    validate_one_skipped_task
)


CMD = (
    'STOLOS_TASKS_JSON={tasks_json}'
    ' STOLOS_APP_NAME={app_name}'
    ' STOLOS_QB_ZOOKEEPER_HOSTS=localhost:2181'
    ' python -m stolos.runner '
    ' {extra_opts}'
)


def run_code(log, tasks_json_tmpfile, app_name, extra_opts='',
             capture=False, raise_on_err=True, async=False):
    """Execute a shell command that runs Stolos for a given app_name

    `async` - (bool) return Popen process instance.  other kwargs do not apply
    `capture` - (bool) return (stdout, stderr)
    """
    cmd = CMD.format(
        app_name=app_name,
        tasks_json=tasks_json_tmpfile,
        extra_opts=extra_opts)
    log.debug('run code', extra=dict(cmd=cmd))
    p = subprocess.Popen(
        cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    if async:
        return p

    stdout, stderr = p.communicate()
    rc = p.poll()
    if raise_on_err and rc:
        raise Exception(
            "command failed. returncode: %s\ncmd: %s\nstderr: %s\nstdout: %s\n"
            % (rc, cmd, stderr, stdout))
    log.warn("Ran a shell command and got this stdout and stderr: "
             " \nSTDOUT:\n%s \nSTDERR:\n %s"
             % (stdout, stderr))
    if capture:
        return stdout, stderr


@with_setup
def test_maybe_add_subtask_no_priority(app1, job_id1, job_id2):
    api.maybe_add_subtask(app1, job_id1)
    api.maybe_add_subtask(app1, job_id2)
    nose.tools.assert_equal(consume_queue(app1), job_id1)
    nose.tools.assert_equal(consume_queue(app1), job_id2)


@with_setup
def test_maybe_add_subtask_priority_first(app1, job_id1, job_id2):
    api.maybe_add_subtask(app1, job_id1, priority=10)
    api.maybe_add_subtask(app1, job_id2, priority=20)
    nose.tools.assert_equal(consume_queue(app1), job_id1)
    nose.tools.assert_equal(consume_queue(app1), job_id2)


@with_setup
def test_maybe_add_subtask_priority_second(app1, job_id1, job_id2):
    api.maybe_add_subtask(app1, job_id1, priority=20)
    api.maybe_add_subtask(app1, job_id2, priority=10)
    nose.tools.assert_equal(consume_queue(app1), job_id2)
    nose.tools.assert_equal(consume_queue(app1), job_id1)


@with_setup
def test_bypass_scheduler(bash1, job_id1, log, tasks_json_tmpfile):
    validate_zero_queued_task(bash1)
    run_code(
        log, tasks_json_tmpfile, bash1,
        '--bypass_scheduler --job_id %s --bash_cmd echo 123' % job_id1)
    validate_zero_queued_task(bash1)
    validate_not_exists(bash1, job_id1)


@with_setup
def test_no_tasks(app1, app2, log, tasks_json_tmpfile):
    # The script shouldn't fail if it doesn't find any queued tasks
    run_code(log, tasks_json_tmpfile, app1)
    validate_zero_queued_task(app1)
    validate_zero_queued_task(app2)


@with_setup
def test_create_child_task_after_one_parent_completed(
        app1, app2, app3, job_id1, log, tasks_json_tmpfile, func_name):
    # if you modify the tasks.json file in the middle of processing the dag
    # modifications to the json file should be recognized

    # the child task should run if another parent completes
    # but otherwise should not run until it's manually queued

    qb.set_state(app1, job_id1, completed=True)
    validate_one_completed_task(app1, job_id1)

    injected_app = app3
    dct = {
        injected_app: {
            "job_type": "bash",
            "depends_on": {"app_name": [app1, app2]},
        },
    }

    with inject_into_dag(func_name, dct):
        validate_zero_queued_task(injected_app)
        # unnecessary side effect: app1 queues app2...
        consume_queue(app2)
        qb.set_state(app2, job_id1, completed=True)

        validate_one_completed_task(app2, job_id1)
        validate_one_queued_task(injected_app, job_id1)
        run_code(log, tasks_json_tmpfile, injected_app, '--bash_cmd echo 123')
        validate_one_completed_task(injected_app, job_id1)


@with_setup
def test_create_parent_task_after_child_completed(app1, app3, job_id1,
                                                  func_name):
    # if you modify the tasks.json file in the middle of processing the dag
    # modifications to the json file should be recognized appropriately

    # we do not re-schedule the child unless parent is completed

    qb.set_state(app1, job_id1, completed=True)
    validate_one_completed_task(app1, job_id1)

    injected_app = app3
    child_injapp = 'test_stolos/%s/testX' % func_name
    dct = {
        injected_app: {
            "job_type": "bash",
        },
        child_injapp: {
            "job_type": "bash",
            "depends_on": {"app_name": [injected_app]}
        }
    }
    with inject_into_dag(func_name, dct):
        validate_zero_queued_task(injected_app)
        qb.set_state(injected_app, job_id1, completed=True)
        validate_one_completed_task(injected_app, job_id1)
        validate_one_queued_task(child_injapp, job_id1)


@with_setup
def test_should_not_add_queue_while_consuming_queue(app1, job_id1):
    # This test guards from doubly queuing jobs
    # This protects from simultaneous operations on root and leaf nodes
    # ie (parent and child) for the following operations:
    # adding, readding or a mix of both
    enqueue(app1, job_id1)

    q = qb.get_qbclient().LockingQueue(app1)
    q.get()
    validate_one_queued_task(app1, job_id1)

    enqueue(app1, job_id1)
    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        qb.readd_subtask(app1, job_id1)
    validate_one_queued_task(app1, job_id1)


@with_setup
def test_push_tasks(app1, app2, job_id1, log, tasks_json_tmpfile):
    """
    Child tasks should be generated and executed properly

    if task A --> task B, and we queue & run A,
    then we should end up with one A task completed and one queued B task
    """
    enqueue(app1, job_id1)
    run_code(log, tasks_json_tmpfile, app1)
    validate_one_completed_task(app1, job_id1)
    # check child
    validate_one_queued_task(app2, job_id1)


@with_setup
def test_rerun_pull_tasks(app1, app2, job_id1, log, tasks_json_tmpfile):
    # queue and complete app 1. it queues a child
    enqueue(app1, job_id1)
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    validate_zero_queued_task(app1)
    validate_one_queued_task(app2, job_id1)
    # complete app 2
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_zero_queued_task(app2)

    # readd app 2
    qb.readd_subtask(app2, job_id1)
    validate_zero_queued_task(app1)
    validate_one_queued_task(app2, job_id1)
    # run app 2.  the parent was previously completed
    run_code(log, tasks_json_tmpfile, app2)
    validate_one_completed_task(app1, job_id1)  # previously completed
    validate_one_completed_task(app2, job_id1)


@with_setup
def test_rerun_manual_task(app1, job_id1):
    enqueue(app1, job_id1)
    validate_one_queued_task(app1, job_id1)

    with nose.tools.assert_raises(exceptions.JobAlreadyQueued):
        qb.readd_subtask(app1, job_id1)


@with_setup
def test_rerun_manual_task2(app1, job_id1):
    qb.readd_subtask(app1, job_id1)
    validate_one_queued_task(app1, job_id1)


@with_setup
def test_rerun_push_tasks_when_manually_queuing_child_and_parent(
        app1, app2, job_id1):
    _test_rerun_tasks_when_manually_queuing_child_and_parent(
        app1, app2, job_id1)

    # complete parent first
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)

    # child completes normally
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)


@with_setup
def test_rerun_pull_tasks_when_manually_queuing_child_and_parent(
        app1, app2, job_id1):
    _test_rerun_tasks_when_manually_queuing_child_and_parent(
        app1, app2, job_id1)

    # complete child first
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    # --> parent still queued
    validate_one_queued_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)

    # then complete parent
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    # --> child gets re-queued
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)

    # complete child second time
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)


def _test_rerun_tasks_when_manually_queuing_child_and_parent(
        app1, app2, job_id1):
    # complete parent and child
    enqueue(app1, job_id1)
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)

    # manually re-add child
    qb.readd_subtask(app2, job_id1)
    validate_one_queued_task(app2, job_id1)
    validate_one_completed_task(app1, job_id1)

    # manually re-add parent
    qb.readd_subtask(app1, job_id1)
    validate_one_queued_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)


@with_setup
def test_rerun_push_tasks(app1, app2, job_id1):
    # this tests recursively deleteing parent status on child nodes

    # queue and complete app 1. it queues a child
    enqueue(app1, job_id1)
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)

    # complete app 2
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)

    # readd app 1
    qb.readd_subtask(app1, job_id1)
    validate_one_queued_task(app1, job_id1)
    validate_zero_queued_task(app2)
    nose.tools.assert_true(
        qb.check_state(app2, job_id1, pending=True))

    # complete app 1
    qb.set_state(app1, job_id1, completed=True)
    consume_queue(app1)
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    # complete app 2
    qb.set_state(app2, job_id1, completed=True)
    consume_queue(app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)


@with_setup
def test_complex_dependencies_pull_push(
        depends_on1, depends_on_job_id1, log, tasks_json_tmpfile):
    job_id = depends_on_job_id1
    enqueue(depends_on1, job_id)
    run_code(log, tasks_json_tmpfile, depends_on1, '--bash_cmd echo 123')

    parents = api.get_parents(depends_on1, job_id)
    parents = list(api.topological_sort(parents))
    for parent, pjob_id in parents[:-1]:
        qb.set_state(parent, pjob_id, completed=True)
        validate_zero_queued_task(depends_on1)
    qb.set_state(*parents[-1], completed=True)
    validate_one_queued_task(depends_on1, job_id)
    run_code(log, tasks_json_tmpfile, depends_on1, '--bash_cmd echo 123')
    validate_one_completed_task(depends_on1, job_id)


@with_setup
def test_complex_dependencies_readd(depends_on1, depends_on_job_id1,
                                    log, tasks_json_tmpfile):
    job_id = depends_on_job_id1

    # mark everything completed
    parents = list(api.topological_sort(api.get_parents(depends_on1, job_id)))
    for parent, pjob_id in parents:
        qb.set_state(parent, pjob_id, completed=True)
    # --> parents should queue our app
    validate_one_queued_task(depends_on1, job_id)
    consume_queue(depends_on1)
    qb.set_state(depends_on1, job_id, completed=True)
    validate_one_completed_task(depends_on1, job_id)

    log.warn("OK... Now try complex dependency test with a readd")
    # re-complete the very first parent.
    # we assume that this parent is a root task
    parent, pjob_id = parents[0]
    qb.readd_subtask(parent, pjob_id)
    validate_one_queued_task(parent, pjob_id)
    validate_zero_queued_task(depends_on1)
    consume_queue(parent)
    qb.set_state(parent, pjob_id, completed=True)
    validate_one_completed_task(parent, pjob_id)
    # since that parent re-queues children that may be depends_on1's
    # parents, complete those too!
    for p2, pjob2 in api.get_children(parent, pjob_id, False):
        if p2 == depends_on1:
            continue
        consume_queue(p2)
        qb.set_state(p2, pjob2, completed=True)
    # now, that last parent should have queued our application
    validate_one_queued_task(depends_on1, job_id)
    run_code(log, tasks_json_tmpfile, depends_on1, '--bash_cmd echo 123')
    validate_one_completed_task(depends_on1, job_id)
    # phew!


@with_setup
def test_pull_tasks(app1, app2, job_id1, log, tasks_json_tmpfile):
    """
    Parent tasks should be generated and executed before child tasks
    (The Bubble Up and then Bubble Down test)

    If A --> B, and:
        we queue and run B, then we should have 0 completed tasks,
        but A should be queued

        nothing should change until:
            we run A and A becomes completed
            we then run B and B becomes completed
    """
    enqueue(app2, job_id1)
    run_code(log, tasks_json_tmpfile, app2)
    validate_one_queued_task(app1, job_id1)
    validate_zero_queued_task(app2)

    run_code(log, tasks_json_tmpfile, app2)
    validate_one_queued_task(app1, job_id1)
    validate_zero_queued_task(app2)

    run_code(log, tasks_json_tmpfile, app1)
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    run_code(log, tasks_json_tmpfile, app2)
    validate_one_completed_task(app2, job_id1)


@with_setup
def test_pull_tasks_with_many_children(app1, app2, app3, app4, job_id1,
                                       log, tasks_json_tmpfile):
    enqueue(app4, job_id1)
    validate_one_queued_task(app4, job_id1)
    validate_zero_queued_task(app1)
    validate_zero_queued_task(app2)
    validate_zero_queued_task(app3)

    run_code(log, tasks_json_tmpfile, app4, '--bash_cmd echo app4helloworld')
    validate_zero_queued_task(app4)
    validate_one_queued_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    validate_one_queued_task(app3, job_id1)

    consume_queue(app1)
    qb.set_state(app1, job_id1, completed=True)
    validate_zero_queued_task(app4)
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    validate_one_queued_task(app3, job_id1)

    consume_queue(app2)
    qb.set_state(app2, job_id1, completed=True)
    validate_zero_queued_task(app4)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)
    validate_one_queued_task(app3, job_id1)

    consume_queue(app3)
    qb.set_state(app3, job_id1, completed=True)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)
    validate_one_completed_task(app3, job_id1)
    validate_one_queued_task(app4, job_id1)

    consume_queue(app4)
    qb.set_state(app4, job_id1, completed=True)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)
    validate_one_completed_task(app3, job_id1)
    validate_one_completed_task(app4, job_id1)


@with_setup
def test_retry_failed_task(
        app1, job_id1, job_id2, log, tasks_json_tmpfile):
    """
    Retry failed tasks up to max num retries and then remove self from queue

    Tasks should maintain proper task state throughout.
    """
    # create 2 tasks in same queue
    enqueue(app1, job_id1)
    enqueue(app1, job_id2, validate_queued=False)
    nose.tools.assert_equal(2, get_qb_status(app1, job_id1)['app_qsize'])
    nose.tools.assert_equal(job_id1, cycle_queue(app1))
    # run job_id2 and have it fail
    run_code(
        log, tasks_json_tmpfile, app1,
        extra_opts='--bash_cmd "&& notacommand...fail" ')
    # ensure we still have both items in the queue
    nose.tools.assert_true(get_qb_status(app1, job_id1)['in_queue'])
    nose.tools.assert_true(get_qb_status(app1, job_id2)['in_queue'])
    # ensure the failed task is sent to back of the queue
    nose.tools.assert_equal(2, get_qb_status(app1, job_id1)['app_qsize'])
    nose.tools.assert_equal(job_id1, cycle_queue(app1))
    # run and fail n times, where n = max failures
    run_code(
        log, tasks_json_tmpfile, app1,
        extra_opts='--max_retry 1 --bash_cmd "&& notacommand...fail"')
    # verify that job_id2 is removed from queue
    validate_one_queued_task(app1, job_id1)
    # verify that job_id2 state is 'failed' and job_id1 is still pending
    validate_one_failed_task(app1, job_id2)


@with_setup
def test_valid_if_or(app2, job_id1):
    """Invalid tasks should be automatically completed.
    This is a valid_if_or test  (aka passes_filter )... bad naming sorry!"""
    job_id = job_id1.replace('profile', 'content')
    enqueue(app2, job_id, validate_queued=False)
    validate_one_skipped_task(app2, job_id)


@with_setup
def test_valid_if_or_func1(app3, job_id1, job_id2, job_id3):
    """Verify that the valid_if_or option supports the "_func" option

    app_name: {"valid_if_or": {"_func": "python.import.path.to.func"}}
    where the function definition looks like: func(**parsed_job_id)
    """
    enqueue(app3, job_id2, validate_queued=False)
    validate_one_skipped_task(app3, job_id2)


@with_setup
def test_valid_if_or_func2(app3, job_id1, job_id2, job_id3):
    """Verify that the valid_if_or option supports the "_func" option

    app_name: {"valid_if_or": {"_func": "python.import.path.to.func"}}
    where the function definition looks like: func(**parsed_job_id)
    """
    enqueue(app3, job_id3, validate_queued=False)
    validate_one_skipped_task(app3, job_id3)


@with_setup
def test_valid_if_or_func3(app3, job_id1, job_id2, job_id3):
    """Verify that the valid_if_or option supports the "_func" option

    app_name: {"valid_if_or": {"_func": "python.import.path.to.func"}}
    where the function definition looks like: func(**parsed_job_id)
    """
    # if the job_id matches the valid_if_or: {"_func": func...} criteria, then:
    enqueue(app3, job_id1, validate_queued=False)
    validate_one_queued_task(app3, job_id1)


@with_setup
def test_valid_task(app2, job_id1):
    """Valid tasks should be automatically completed"""
    enqueue(app2, job_id1, validate_queued=True)


@with_setup
def test_bash(bash1, job_id1, log, tasks_json_tmpfile):
    """a bash task should execute properly """
    # queue task
    enqueue(bash1, job_id1)
    validate_one_queued_task(bash1, job_id1)
    # run failing task
    run_code(
        log, tasks_json_tmpfile, bash1, '--bash_cmd thiscommandshouldfail')
    validate_one_queued_task(bash1, job_id1)
    # run successful task
    run_code(log, tasks_json_tmpfile, bash1, '--bash_cmd echo 123')
    validate_zero_queued_task(bash1)


@with_setup
def test_app_has_command_line_params(
        bash1, job_id1, log, tasks_json_tmpfile):
    enqueue(bash1, job_id1)
    msg = 'output: %s'
    # Test passed in params exist
    _, logoutput = run_code(
        log, tasks_json_tmpfile, bash1,
        extra_opts='--redirect_to_stderr --bash_cmd echo newfakereadfp',
        capture=True, raise_on_err=True)
    nose.tools.assert_in(
        'newfakereadfp', logoutput, msg % logoutput)


@with_setup
def test_run_given_specific_job_id(app1, job_id1, log, tasks_json_tmpfile):
    enqueue(app1, job_id1)
    out, err = run_code(
        log, tasks_json_tmpfile, app1,
        '--job_id %s' % job_id1, raise_on_err=False, capture=True)
    nose.tools.assert_regexp_matches(err, (
        'UserWarning: Will not execute this task because it might be'
        ' already queued or completed!'))
    validate_one_queued_task(app1, job_id1)


@with_setup
def test_child_running_while_parent_pending_but_not_executing(
        app1, app2, job_id1):
    enqueue(app1, job_id1)
    enqueue(app2, job_id1)
    parents_completed, consume_queue, parent_lock = \
        qb.ensure_parents_completed(app2, job_id1)
    # ensure lock is obtained by ensure_parents_completed
    validate_one_queued_executing_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    nose.tools.assert_equal(parents_completed, False)
    # child should promise to remove itself from queue
    nose.tools.assert_equal(consume_queue, True)
    nose.tools.assert_is_instance(parent_lock, qb.BaseLock)


@with_setup
def test_child_running_while_parent_pending_and_executing(
        app1, app2, job_id1):
    enqueue(app1, job_id1)
    enqueue(app2, job_id1)
    lock = qb.obtain_execute_lock(app1, job_id1)
    assert lock
    parents_completed, consume_queue, parent_lock = \
        qb.ensure_parents_completed(app2, job_id1)
    validate_one_queued_executing_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)
    nose.tools.assert_equal(parents_completed, False)
    # child should not promise to remove itself from queue
    nose.tools.assert_equal(consume_queue, False)
    nose.tools.assert_is_none(parent_lock)


@with_setup
def test_race_condition_when_parent_queues_child(
        app1, app2, job_id1, log, tasks_json_tmpfile):
    # The parent queues the child and the child runs before the parent gets
    # a chance to mark itself as completed
    qb.set_state(app1, job_id1, pending=True)
    lock = qb.obtain_execute_lock(app1, job_id1)
    assert lock
    qb.set_state(app1, job_id1, completed=True)
    qb.set_state(app1, job_id1, pending=True)
    validate_one_queued_task(app2, job_id1)
    validate_zero_queued_task(app1)

    # should not complete child.  should de-queue child
    # should not queue parent.
    # should exit gracefully
    run_code(log, tasks_json_tmpfile, app2)
    validate_zero_queued_task(app1)
    validate_one_queued_task(app2, job_id1)

    qb.set_state(app1, job_id1, completed=True)
    lock.release()
    validate_one_completed_task(app1, job_id1)
    validate_one_queued_task(app2, job_id1)

    run_code(log, tasks_json_tmpfile, app2)
    validate_one_completed_task(app1, job_id1)
    validate_one_completed_task(app2, job_id1)


@with_setup
def test_run_multiple_given_specific_job_id(
        bash1, job_id1, log, tasks_json_tmpfile):
    p = run_code(
        log, tasks_json_tmpfile, bash1,
        extra_opts='--job_id %s --timeout 1 --bash_cmd sleep 2' % job_id1,
        async=True)
    p2 = run_code(
        log, tasks_json_tmpfile, bash1,
        extra_opts='--job_id %s --timeout 1 --bash_cmd sleep 2' % job_id1,
        async=True)
    # one of them should fail.  both should run asynchronously
    err = p.communicate()[1] + p2.communicate()[1]
    statuses = [p.poll(), p2.poll()]
    # one job succeeds.  one job fails
    nose.tools.assert_regexp_matches(err, 'successfully completed job')
    nose.tools.assert_regexp_matches(err, (
        '(UserWarning: Will not execute this task because it might'
        ' be already queued or completed!'
        '|Lock already acquired)'))
    # failing job should NOT gracefully quit
    nose.tools.assert_equal(
        list(sorted(statuses)), [0, 1], msg="expected exactly one job to fail")


@with_setup
def test_run_failing_spark_given_specific_job_id(
        bash1, job_id1, log, tasks_json_tmpfile):
    """
    task should still get queued if --job_id is specified and the task fails
    """
    with nose.tools.assert_raises(Exception):
        run_code(log, tasks_json_tmpfile, bash1, '--pluginfail')
    validate_zero_queued_task(bash1)
    run_code(
        log, tasks_json_tmpfile, bash1,
        '--job_id %s --bash_cmd kasdfkajsdfajaja' % job_id1)
    validate_one_queued_task(bash1, job_id1)


@with_setup
def test_failing_task(bash1, job_id1, log, tasks_json_tmpfile):
    _, err = run_code(
        log, tasks_json_tmpfile, bash1,
        ' --job_id %s --bash_cmd notacommand...fail' % job_id1,
        capture=True)
    nose.tools.assert_regexp_matches(
        err, "Bash job failed")
    nose.tools.assert_regexp_matches(
        err, "Task retry count increased")

    _, err = run_code(
        log, tasks_json_tmpfile, bash1,
        '--max_retry 1 --bash_cmd jaikahhaha', capture=True)
    nose.tools.assert_regexp_matches(
        err, "Task retried too many times and is set as permanently failed")


@with_setup
def test_invalid_queued_job_id(app4, depends_on_job_id1,
                               log, tasks_json_tmpfile):
    job_id = depends_on_job_id1  # this job_id does not match the app
    # manually bypass the decorator that validates job_id
    qb._set_state_unsafe(app4, job_id, pending=True)
    q = qb.get_qbclient().LockingQueue(app4)
    q.put(job_id)
    validate_one_queued_task(app4, job_id)

    run_code(log, tasks_json_tmpfile, app4, '--bash_cmd echo 123')
    validate_one_failed_task(app4, job_id)
    validate_zero_queued_task(app4)
