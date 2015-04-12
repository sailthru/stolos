"""
TODO:
    - remove zookeeper language from here
    - have the functions expect to call methods from an object that implements
      QB api
    - add check_state and set_state to task_states?
"""


@util.pre_condition(dag_tools.parse_job_id)
def readd_subtask(app_name, job_id, timeout=5, _force=False,
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
    # obtain lock
    try:
        lock = obtain_add_lock(
            app_name, job_id, timeout=timeout, raise_on_error=True)
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
    try:
        if _force:
            queued = False
        else:
            queued = check_if_queued(
                app_name, job_id)
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


def get_job_path(app_name, job_id, *args):
    return join(app_name, 'all_subtasks', job_id, *args)


def _get_lockpath(typ, app_name, job_id):
    assert typ in set(['execute', 'add'])
    return join(get_job_path(app_name, job_id), '%s_lock' % typ)


def obtain_add_lock(app_name, job_id, *args, **kwargs):
    """Obtain a lock used when adding a job to queue"""
    return _obtain_lock(
        'add', app_name=app_name, job_id=job_id, *args, **kwargs)


def obtain_execute_lock(app_name, job_id, *args, **kwargs):
    """Obtain a lock used when executing a job"""
    return _obtain_lock(
        'execute', app_name=app_name, job_id=job_id, *args, **kwargs)


def _maybe_queue_children(parent_app_name, parent_job_id):
    """
    This is basically a "set_state(completed=True)" pre-commit hook

    Assume the task identified by (parent_app_name, parent_job_id) is
    completed, and for each of that parent's children in the dag graph of
    tasks, set 1/num_parents worth of points towards that child's completion.

    If any one child has earned 1 point, then add it to its task queue

    We track the "score" of a child by counting files in the zookeeper path:
        .../parents/dependency_name/parent_app_name/parent_job_id
    """
    gen = dag_tools.get_children(parent_app_name, parent_job_id, True)

    # cache data in form: {parent: is_complete}
    state_dct = {(parent_app_name, parent_job_id): True}

    for child_app_name, cjob_id, dep_grp in gen:
        parents = list(dag_tools.get_parents(child_app_name, cjob_id))
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


def ensure_parents_completed(app_name, job_id, timeout):
    """
    Assume that given job_id is pulled from the app_name's queue.

    Check that the parent tasks for this (app_name, job_id) pair have completed
    If they haven't completed and aren't pending, maybe create the
    parent task in its appropriate queue.  Also decide whether the calling
    process should requeue given job_id or remove itself from queue.

    Returns a tuple:
        (are_parents_completed, should_job_id_be_consumed_from_queue,
         parent_execute_locks_to_release)
    """
    parents_completed = True
    consume_queue = False
    parent_locks = set()
    for parent, pjob_id, dep_grp in dag_tools.get_parents(app_name,
                                                          job_id, True):
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
        # in me cycling through the queue a couple times.

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

        elock = obtain_execute_lock(
            parent, pjob_id,
            raise_on_error=False, timeout=timeout)
        if elock:
            if not check_state(parent, pjob_id, pending=True):
                elock.release()  # race condition
            else:
                consume_queue = True
                parent_locks.add(elock)
                log.info(
                    "I will unqueue myself with the expectation that"
                    " my parent will requeue me", extra=dict(
                        app_name=app_name, job_id=job_id))
    return parents_completed, consume_queue, parent_locks
