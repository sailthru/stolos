from kazoo.client import KazooClient
import kazoo.exceptions
from os.path import join
import os

from scheduler import util
from scheduler import log
from scheduler import dag_tools
from scheduler import exceptions


ZOOKEEPER_PENDING = 'pending'
ZOOKEEPER_COMPLETED = 'completed'
ZOOKEEPER_FAILED = 'failed'
ZOOKEEPER_SKIPPED = 'skipped'


@util.cached
def get_client(zookeeper_hosts=None):
    if zookeeper_hosts is None:
        zookeeper_hosts = os.environ["ZOOKEEPER_HOSTS"]
    log.debug("Connecting to ZooKeeper: %s" % zookeeper_hosts)
    zk = KazooClient(zookeeper_hosts)
    zk.start()
    zk.logger.handlers = log.handlers
    zk.logger.setLevel('WARN')
    return zk


def _queue(app_name, job_id, zk, queue=True):
    """ Calling code should obtain a lock first!
    If queue=False, do everything except queue (ie set state)"""
    log.info('Creating and queueing new subtask',
             extra=dict(app_name=app_name, job_id=job_id))

    if dag_tools.passes_filter(app_name, job_id):
        # hack: zookeeper doesn't like unicode
        if isinstance(job_id, unicode):
            job_id = str(job_id)
        if queue:
            zk.LockingQueue(app_name).put(job_id)
        else:
            log.warn(
                'create a subtask but not actually queueing it',
                extra=dict(app_name=app_name, job_id=job_id))
        set_state(app_name, job_id, zk, pending=True)
    else:
        log.info(
            'job invalid.  marking as skipped so it does not run',
            extra=dict(app_name=app_name, job_id=job_id))
        set_state(app_name, job_id, zk, skipped=True)


@util.pre_condition(dag_tools.parse_job_id)
def readd_subtask(app_name, job_id, zk, timeout=5,
                  _reset_descendants=True, _ignore_if_queued=False):
    """
    Queue a new task if it isn't already in the queue.

    This is slow if the queue is large.  Use carefully!

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
        lock = obtain_lock(
            app_name, job_id, zk, timeout=timeout, raise_on_error=True)
    except exceptions.CouldNotObtainLock:
        # call maybe_add_subtask(...) and return
        if not maybe_add_subtask(app_name, job_id, zk=zk):
            raise exceptions.CodeError(
                "wtf?  If I can't obtain a lock on a job_id, then I should"
                " be able to maybe_add_subtask.")
        return

    try:
        p = join(app_name, 'entries')
        try:
            queued_jobs = {zk.get(join(p, x))[0] for x in zk.get_children(p)}
        except kazoo.exceptions.NoNodeError:
            # if the task was never actually added, we'll add it for the first
            # time.  The child's parents are marked as
            # completed, but the child hasn't run yet
            queued_jobs = set()

        do_not_queue = False
        if job_id in queued_jobs:
            if _ignore_if_queued:
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
                do_not_queue = True
            else:
                raise exceptions.JobAlreadyQueued(
                    "%s %s" % (app_name, job_id))

        if _reset_descendants:
            # all child tasks will also get re-executed
            _recursively_reset_child_task_state(app_name, job_id, zk=zk)

        if not do_not_queue:
            _queue(app_name, job_id, zk)
    finally:
        lock.release()


@util.pre_condition(dag_tools.parse_job_id)
def maybe_add_subtask(app_name, job_id, zk=None, zookeeper_hosts=None,
                      timeout=5, queue=True):
    """Add a subtask to the queue if it hasn't been added yet"""
    if zk is None:
        zk = get_client(zookeeper_hosts)
    if zk.exists(_get_zookeeper_path(app_name, job_id)):
        return False
    # get a lock so we guarantee this task isn't being added twice
    lock = obtain_lock(app_name, job_id, zk, timeout=timeout, safe=False)
    if not lock:
        return False
    try:
        _queue(app_name, job_id, zk, queue=queue)
    finally:
        lock.release()
    return True


def obtain_lock(app_name, job_id, zk, timeout=None, blocking=True,
                raise_on_error=False, safe=True):
    """Try to acquire a lock.

    `safe` (bool) By default, this function is `safe` because it will not
    create a node for the job_id if it doesn't already exist.
    `raise_on_error` (bool) if False, just return False.
                     only applicable if `safe` is True.
    `timeout` and `blocking` are options passed to lock.aquire(...)

    Either return a lock or [ raise | return False ]
    """
    _path = _get_zookeeper_path(app_name, job_id)
    if safe and not zk.exists(_path):
        log.warn(
            "Cannot create a lock if the task hasn't been added yet!",
            extra=dict(app_name=app_name, job_id=job_id))
        if raise_on_error:
            raise exceptions.CouldNotObtainLock(
                'You must create a job_id before obtaining a lock on it!'
                ' %s %s' % (app_name, job_id))
        else:
            return False

    path = join(_path, 'lock')
    l = zk.Lock(path)
    try:
        l.acquire(timeout=timeout, blocking=blocking)
    except:
        if raise_on_error:
            raise exceptions.LockAlreadyAcquired(
                'Lock already acquired. %s %s' % (app_name, job_id))
        else:
            log.warn(
                "Lock already acquired.",
                extra=dict(app_name=app_name, job_id=job_id))
            return False
    return l


@util.pre_condition(dag_tools.parse_job_id)
def inc_retry_count(app_name, job_id, zk, max_retry):
    """Increment the retry count for the given task.  If the retry count is
    greater than the max allowed number of retries, set the tasks's state
    to failed.

    Returns False if task exceeded retry limit and True if the increment was
    fine
    """
    path = join(_get_zookeeper_path(app_name, job_id), 'retry_count')
    if not zk.exists(path):
        zk.create(path, '0', makepath=False)
        cnt = 0
    else:
        cnt = int(zk.get(path)[0])
    if cnt + 1 >= max_retry:
        set_state(app_name, job_id, zk, failed=True)
        log.error(
            'Task retried too many times and is set as permanantly failed.',
            extra=dict(retry_cnt=cnt, app_name=app_name, job_id=job_id))
        exceeded_limit = True
    else:
        exceeded_limit = False
    zk.set(path, str(cnt + 1))
    log.info('Task retry count increased',
             extra=dict(retry_cnt=cnt + 1, app_name=app_name, job_id=job_id))
    return exceeded_limit


def _get_zookeeper_path(app_name, job_id, *args):
    return join(app_name, 'all_subtasks', job_id, *args)


def _validate_state(pending, completed, failed, skipped):
    assert pending + completed + failed + skipped == 1
    if pending:
        state = ZOOKEEPER_PENDING
    elif completed:
        state = ZOOKEEPER_COMPLETED
    elif failed:
        state = ZOOKEEPER_FAILED
    elif skipped:
        state = ZOOKEEPER_SKIPPED
    return state


def _maybe_queue_children(parent_app_name, parent_job_id, zk):
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
                state_dct, (p, pj), check_state, p, pj, zk=zk, completed=True))

        ld = dict(
            child_app_name=child_app_name,
            parent_app_name=parent_app_name,
            job_id=cjob_id)
        if (pcomplete == ptotal):
            log.info(
                "Parent is queuing a child task", extra=ld)
            if check_state(child_app_name, cjob_id, zk=zk, completed=True):
                log.warn(
                    "Queuing a previously completed child task"
                    " presumably because of the following:"
                    " 1) you manually queued both a"
                    " parent/ancestor and the child,"
                    " and 2) the child completed first."
                    " You probably shouldn't manually re-queue both parents"
                    " and children. Just queue one of them.",
                    extra=ld)

            readd_subtask(
                child_app_name, cjob_id, zk=zk,
                _reset_descendants=False,  # descendants previously handled
                _ignore_if_queued=True
            )
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


def _recursively_reset_child_task_state(parent_app_name, job_id, zk):
    log.debug(
        "recursively setting all descendant tasks to 'pending' and "
        " marking that the parent is not completed",
        extra=dict(app_name=parent_app_name, job_id=job_id))

    gen = dag_tools.get_children(parent_app_name, job_id, True)
    for child_app_name, cjob_id, dep_grp in gen:
        child_path = _get_zookeeper_path(child_app_name, cjob_id)
        if zk.exists(child_path):
            set_state(child_app_name, cjob_id, zk, pending=True)
            _recursively_reset_child_task_state(child_app_name, cjob_id, zk)
        else:
            pass  # no need to recurse further down the tree


@util.pre_condition(dag_tools.parse_job_id)
def set_state(app_name, job_id, zk,
              pending=False, completed=False, failed=False, skipped=False):
    """
    Set the state of a task

    `app_name` is a task identifier
    `job_id` is a subtask identifier
    `zk` is a KazooClient instance
    `pending`, `completed` and `failed` (bool) are mutually exclusive
    """
    zookeeper_path = _get_zookeeper_path(app_name, job_id)
    state = _validate_state(pending, completed, failed, skipped)
    if completed:  # basecase
        _maybe_queue_children(
            parent_app_name=app_name, parent_job_id=job_id, zk=zk)

    if zk.exists(zookeeper_path):
        zk.set(zookeeper_path, state)
    else:
        zk.create(zookeeper_path, state, makepath=True)
    log.debug(
        "Set task state",
        extra=dict(state=state, app_name=app_name, job_id=job_id))

_set_state_unsafe = set_state.func_closure[0].cell_contents


def check_state(app_name, job_id, zk, raise_if_not_exists=False,
                pending=False, completed=False, failed=False, skipped=False,
                _get=False):
    """Determine whether a specific task has been completed yet.

    `app_name` is a task identifier
    `job_id` is a subtask identifier
    `zk` is a KazooClient instance
    `_get` (bool) if True, just return the string value of the state and
                  ignore the (pending, completed, xor failed) choice
    """
    zookeeper_path = _get_zookeeper_path(app_name, job_id)
    try:
        gotstate = zk.get(zookeeper_path)[0]
    except kazoo.exceptions.NoNodeError:
        if raise_if_not_exists:
            raise
        else:
            return False
    if _get:
        return gotstate
    else:
        expected_state = _validate_state(pending, completed, failed, skipped)
        return gotstate == expected_state
