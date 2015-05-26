from os.path import join

from stolos import dag_tools as dt
from stolos import util
from stolos import exceptions

from .locking import obtain_add_lock, obtain_execute_lock
from .read_job_state import check_state, validate_state
from . import shared
from . import log


def _queue(app_name, job_id, queue=True, priority=None):
    """ Calling code should obtain a lock first!
    If queue=False, do everything except queue (ie set state)"""
    qbcli = shared.get_qbclient()
    log.info(
        'Creating and queueing new subtask',
        extra=dict(app_name=app_name, job_id=job_id, priority=priority))
    if dt.passes_filter(app_name, job_id):
        # hack: zookeeper doesn't like unicode
        if isinstance(job_id, unicode):
            job_id = str(job_id)
        if queue:
            if priority:
                qbcli.LockingQueue(app_name).put(job_id, priority=priority)
            else:
                qbcli.LockingQueue(app_name).put(job_id)
        else:
            log.warn(
                'create a subtask but not actually queueing it', extra=dict(
                    app_name=app_name, job_id=job_id, priority=priority))
        set_state(app_name, job_id, pending=True)
    else:
        log.info(
            'job invalid.  marking as skipped so it does not run',
            extra=dict(app_name=app_name, job_id=job_id))
        set_state(app_name, job_id, skipped=True)


@util.pre_condition(dt.parse_job_id)
def maybe_add_subtask(app_name, job_id, queue=True, priority=None):
    """Add a subtask to the queue if it hasn't been added yet.

    `queue` (bool, optional) - if False, don't add the subtask to queue
    `priority` (int, optional) - prioritize this item in the queue.
        1 is highest priority 100 is lowest priority.
        Irrelevant if `queue` is False
    """
    qbcli = shared.get_qbclient()
    if qbcli.exists(shared.get_job_path(app_name, job_id)):
        return False
    # get a lock so we guarantee this task isn't being added twice concurrently
    lock = obtain_add_lock(app_name, job_id, blocking=False, safe=False)
    if not lock:
        return False
    try:
        _queue(app_name, job_id, queue=queue, priority=priority)
    finally:
        lock.release()
    return True


def _recursively_reset_child_task_state(parent_app_name, job_id):
    qbcli = shared.get_qbclient()
    log.debug(
        "recursively setting all descendant tasks to 'pending' and "
        " marking that the parent is not completed",
        extra=dict(app_name=parent_app_name, job_id=job_id))

    gen = dt.get_children(parent_app_name, job_id, True)
    for child_app_name, cjob_id, dep_grp in gen:
        child_path = shared.get_job_path(child_app_name, cjob_id)
        if qbcli.exists(child_path):
            set_state(child_app_name, cjob_id, pending=True)
            _recursively_reset_child_task_state(child_app_name, cjob_id)
        else:
            pass  # no need to recurse further down the tree


@util.pre_condition(dt.parse_job_id)
def readd_subtask(app_name, job_id, _force=False,
                  _reset_descendants=True, _ignore_if_queued=False):
    """
    Queue a new task if it isn't already in the queue.

    This is slow if the queue is large.  Use carefully!
    Potentially unsafe if decendant tasks are already running/queued.

    `_force` (bool) If True, add to queue regardless of whether job_id
        is previously queued
    `_reset_descendants` (bool)  If True, recurse through descendants and reset
        their states to pending
    `_ignore_if_queued` (bool)  If the job_id is previously queued,
        don't re-queue it, but possibly _reset_descendants.
        If False, just raise an error

    In an atomic transaction (ie atomic to this job_id),
    First, validate that the job isn't already queued.
    Then, queue the task
    """
    qbcli = shared.get_qbclient()
    # obtain lock
    try:
        if _ignore_if_queued:  # don't bother waiting for an add lock.
            # Assume that ignore_if_queued == True implies the task might get
            # added to the queue while it is still in the queue.
            #  At this point,
            # we assume it makes no difference whether the task
            # is re-added once
            # or more.  Since multiple processes might be trying to re-add at
            # the same time, just let one of them do it and, at least in this
            # section, raise a failure for the others.
            lock = obtain_add_lock(
                app_name, job_id, blocking=False, raise_on_error=True)
        else:
            lock = obtain_add_lock(
                app_name, job_id, blocking=False, raise_on_error=True)
    except exceptions.CouldNotObtainLock:
        # call maybe_add_subtask(...) and return
        added = maybe_add_subtask(app_name, job_id)
        if not added:
            raise exceptions.CodeError(
                "wtf?  If I can't obtain a lock on a job_id, then I should"
                " be able to maybe_add_subtask.")
        return added
    except exceptions.LockAlreadyAcquired:
        # here's why something else might hold an add lock on the object:
        # - task currently being added by another process
        # - process that was performing above op died a few seconds ago
        raise
    assert lock, "Code Error: should have the add lock at this point"
    try:
        if _force:
            queued = False
        else:
            queued = qbcli.LockingQueue(app_name).is_queued(job_id)
            if queued and _ignore_if_queued:
                log.debug(
                    "Job already queued!  We'll handle this properly."
                    " You may have entered this state"
                    " because you manually re-added a child and parent task"
                    " and then completed the parent task first.  This could"
                    " also happen if, via bubble up, a child queued parents"
                    " and then a parent just tried, via"
                    " bubble down, to queue one of children.", extra=dict(
                        app_name=app_name, job_id=job_id)
                )
            elif queued:
                raise exceptions.JobAlreadyQueued(
                    "%s %s" % (app_name, job_id))

        if _reset_descendants:
            # all child tasks will also get re-executed
            _recursively_reset_child_task_state(app_name, job_id)

        if not queued:
            _queue(app_name, job_id)
    finally:
        lock.release()
    return True


def _maybe_queue_children(parent_app_name, parent_job_id):
    """
    This is basically a "set_state(completed=True)" pre-commit hook

    Assume the task identified by (parent_app_name, parent_job_id) is
    completed, and for each of that parent's children in the dag graph of
    tasks, set 1/num_parents worth of points towards that child's completion.

    If any one child has earned 1 point, then add it to its task queue

    We track the "score" of a child by counting files in the job path:
        .../parents/dependency_name/parent_app_name/parent_job_id
    """
    gen = dt.get_children(parent_app_name, parent_job_id, True)

    # cache data in form: {parent: is_complete}
    state_dct = {(parent_app_name, parent_job_id): True}

    for child_app_name, cjob_id, dep_grp in gen:
        parents = list(dt.get_parents(child_app_name, cjob_id))
        parents.remove((parent_app_name, parent_job_id))
        # get total number of parents
        ptotal = len(parents)
        # get number of parents completed so far.
        # TODO: This check_state operation should be optimized.
        pcomplete = sum(
            1 for p, pj in parents
            if util.lazy_set_default(
                state_dct, (p, pj), check_state, p, pj, completed=True))

        ld = dict(
            child_app_name=child_app_name,
            child_job_id=cjob_id,
            app_name=parent_app_name,
            job_id=parent_job_id)
        if (pcomplete == ptotal):
            log.info(
                "Parent is queuing a child task", extra=ld)
            if check_state(child_app_name, cjob_id, completed=True):
                log.warn(
                    "Queuing a previously completed child task"
                    " presumably because of the following:"
                    " 1) you manually queued both a"
                    " parent/ancestor and the child,"
                    " and 2) the child completed first."
                    " You probably shouldn't manually re-queue both parents"
                    " and children. Just queue one of them.",
                    extra=ld)

            try:
                readd_subtask(
                    child_app_name, cjob_id,
                    _reset_descendants=False,  # descendants previously handled
                    _ignore_if_queued=True)
            except exceptions.JobAlreadyQueued:
                log.info("Child already in queue", extra=dict(**ld))
                raise
        elif (pcomplete < ptotal):
            log.info(
                "Child task is one step closer to being queued!",
                extra=dict(
                    num_complete_dependencies=pcomplete,
                    num_total_dependencies=ptotal, **ld))
        else:
            raise exceptions.CodeError(
                "For some weird reason, I calculated that more parents"
                " completed than there are parents.")


def _set_state_unsafe(
        app_name, job_id,
        pending=False, completed=False, failed=False, skipped=False):
    """
    Set the state of a task

    `app_name` is a task identifier
    `job_id` is a subtask identifier
    `pending`, `completed` and `failed` (bool) are mutually exclusive
    """
    qbcli = shared.get_qbclient()
    job_path = shared.get_job_path(app_name, job_id)
    state = validate_state(pending, completed, failed, skipped)
    if completed:  # basecase
        _maybe_queue_children(parent_app_name=app_name, parent_job_id=job_id)

    if qbcli.exists(job_path):
        qbcli.set(job_path, state)
    else:
        qbcli.create(job_path, state)

    log.debug(
        "Set task state",
        extra=dict(state=state, app_name=app_name, job_id=job_id))

set_state = util.pre_condition(dt.parse_job_id)(
    _set_state_unsafe)


@util.pre_condition(dt.parse_job_id)
def inc_retry_count(app_name, job_id, max_retry):
    """Increment the retry count for the given task.  If the retry count is
    greater than the max allowed number of retries, set the tasks's state
    to failed.

    Returns False if task exceeded retry limit and True if the increment was
    fine
    """
    qbcli = shared.get_qbclient()
    path = join(shared.get_job_path(app_name, job_id), 'retry_count')
    if not qbcli.exists(path):
        qbcli.create(path, '0')
        cnt = 0
    else:
        cnt = int(qbcli.get(path))
    if cnt + 1 >= max_retry:
        set_state(app_name, job_id, failed=True)
        log.error(
            'Task retried too many times and is set as permanently failed.',
            extra=dict(retry_cnt=cnt, app_name=app_name, job_id=job_id))
        exceeded_limit = True
    else:
        exceeded_limit = False
    qbcli.set(path, str(cnt + 1))
    log.info('Task retry count increased',
             extra=dict(retry_cnt=cnt + 1, app_name=app_name, job_id=job_id))
    return exceeded_limit


def ensure_parents_completed(app_name, job_id):
    """
    Assume that given job_id is pulled from the app_name's queue.

    Check that the parent tasks for this (app_name, job_id) pair have completed
    If they haven't completed and aren't pending, maybe create the
    parent task in its appropriate queue.  Also decide whether the calling
    process should requeue given job_id or remove itself from queue.
    Because this needs to happen as one transaction, also return a list of
    execute locks that the calling code must release after it decides how to
    handle the current job_id.

    Returns a tuple:
        (are_parents_completed, should_job_id_be_consumed_from_queue,
         parent_execute_locks_to_release)
    """
    parents_completed = True
    consume_queue = False
    parent_lock = None
    for parent, pjob_id, dep_grp in dt.get_parents(app_name, job_id, True):
        if check_state(app_name=parent, job_id=pjob_id, completed=True):
            continue
        parents_completed = False
        log.info(
            'My parent has not completed yet.',
            extra=dict(
                parent_app_name=parent, parent_job_id=pjob_id,
                app_name=app_name, job_id=job_id))

        # At this point, I need to be re-run
        # The question at this point is whether to requeue myself or assume the
        # parent will.

        # Assume the default is I requeue myself.  Sometimes, this might result
        # in me cycling through the queue a couple times until parent finishes.

        # If parent is running, it will be able to requeue me if I exit in
        # time.  If it doesn't, either I'll requeue myself by default or
        # another parent will.  So, do nothing in this case.

        # if parent is not running, I should try to maybe_add_subtask it.
        # - if can't add parent, then possibly something else is adding it, or
        # it ran once and is waiting on one of my grandparents.
        # - if I can maybe_add_subtask my parent, then it definitely wasn't
        # running before.

        # In both cases,
        # I should try to unqueue myself if I can guarantee that the parent
        # won't run by the time I unqueue myself.  Otherwise, I should just
        # default to assuming parent is running and requeue myself by default.

        maybe_add_subtask(parent, pjob_id)

        if parent_lock is not None:
            continue  # we already found a parent that promises to requeue me

        elock = obtain_execute_lock(
            parent, pjob_id, raise_on_error=False, blocking=False)
        if elock:
            if not check_state(parent, pjob_id, pending=True):
                elock.release()  # race condition: parent just did something!
            else:
                consume_queue = True
                parent_lock = elock
                log.info(
                    "I will unqueue myself with the expectation that"
                    " my parent will requeue me", extra=dict(
                        app_name=app_name, job_id=job_id))
    return parents_completed, consume_queue, parent_lock
