"""
Manages the application queues in zookeeper
"""
import atexit
from kazoo.client import KazooClient
import kazoo.exceptions
from os.path import join
import os

from stolos import util
from stolos import log
from stolos import dag_tools
from stolos import exceptions


ZOOKEEPER_PENDING = 'pending'
ZOOKEEPER_COMPLETED = 'completed'
ZOOKEEPER_FAILED = 'failed'
ZOOKEEPER_SKIPPED = 'skipped'


@util.cached
def get_zkclient(zookeeper_hosts=None):
    """Start and return a connection to ZooKeeper"""
    if zookeeper_hosts is None:
        zookeeper_hosts = os.environ["ZOOKEEPER_HOSTS"]
    log.debug("Connecting to ZooKeeper: %s" % zookeeper_hosts)
    zk = KazooClient(zookeeper_hosts)
    zk.logger.handlers = log.handlers
    zk.logger.setLevel('WARN')
    zk.start()
    atexit.register(zk.stop)
    return zk


def get_qsize(app_name, zk, queued=True, taken=True):
    """
    Find the number of jobs in the given app's queue

    `queued` - Include the entries in the queue that are not currently
        being processed or otherwise locked
    `taken` - Include the entries in the queue that are currently being
        processed or are otherwise locked
    """
    pq = join(app_name, 'entries')
    pt = join(app_name, 'taken')
    if queued:
        entries = len(zk.get_children(pq))
        if taken:
            return entries
        else:
            taken = len(zk.get_children(pt))
            return entries - taken
    else:
        if taken:
            taken = len(zk.get_children(pt))
            return taken
        else:
            raise AttributeError(
                "You asked for an impossible situation.  Queue items are"
                " waiting for a lock xor taken.  You cannot have queue entries"
                " that are both not locked and not waiting.")


def _queue(app_name, job_id, zk, queue=True, priority=None):
    """ Calling code should obtain a lock first!
    If queue=False, do everything except queue (ie set state)"""
    log.info(
        'Creating and queueing new subtask',
        extra=dict(app_name=app_name, job_id=job_id, priority=priority))
    if dag_tools.passes_filter(app_name, job_id):
        # hack: zookeeper doesn't like unicode
        if isinstance(job_id, unicode):
            job_id = str(job_id)
        if queue:
            if priority:
                zk.LockingQueue(app_name).put(job_id, priority=priority)
            else:
                zk.LockingQueue(app_name).put(job_id)
        else:
            log.warn(
                'create a subtask but not actually queueing it', extra=dict(
                    app_name=app_name, job_id=job_id, priority=priority))
        set_state(app_name, job_id, zk, pending=True)
    else:
        log.info(
            'job invalid.  marking as skipped so it does not run',
            extra=dict(app_name=app_name, job_id=job_id))
        set_state(app_name, job_id, zk, skipped=True)


def check_if_queued(app_name, job_id, zk):
    p = join(app_name, 'entries')
    try:
        queued_jobs = {zk.get(join(p, x))[0] for x in zk.get_children(p)}
    except kazoo.exceptions.NoNodeError:
        queued_jobs = set()

    if job_id in queued_jobs:
        return True
    return False


@util.pre_condition(dag_tools.parse_job_id)
def readd_subtask(app_name, job_id, zk, timeout=5, _force=False,
                  _reset_descendants=True, _ignore_if_queued=False):
    """
    Queue a new task if it isn't already in the queue.

    This is slow if the queue is large.  Use carefully!

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
            app_name, job_id, zk, timeout=timeout, raise_on_error=True)
    except exceptions.CouldNotObtainLock:
        # call maybe_add_subtask(...) and return
        added = maybe_add_subtask(
            app_name, job_id, zk=zk)
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
                app_name, job_id, zk)
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
            _recursively_reset_child_task_state(app_name, job_id, zk=zk)

        if not queued:
            _queue(app_name, job_id, zk)
    finally:
        lock.release()
    return True


@util.pre_condition(dag_tools.parse_job_id)
def maybe_add_subtask(app_name, job_id, zk=None, zookeeper_hosts=None,
                      timeout=5, queue=True, priority=None):
    """Add a subtask to the queue if it hasn't been added yet.

    `zk` (kazoo.client.KazooClient instance)
    `zookeeper_hosts` - A zookeeper connection string. Used if `zk` not given.
    `queue` (bool, optional) - if False, don't add the subtask to queue
    `timeout` (int, optional) - num seconds to wait for a lock before queueing
    `priority` (int, optional) - prioritize this item in the queue.
        1 is highest priority 100 is lowest priority.
        Irrelevant if `queue` is False
    """
    if zk is None:
        zk = get_zkclient(zookeeper_hosts)
    if zk.exists(_get_zookeeper_path(app_name, job_id)):
        return False
    # get a lock so we guarantee this task isn't being added twice
    lock = obtain_add_lock(app_name, job_id, zk, timeout=timeout, safe=False)
    if not lock:
        return False
    try:
        _queue(app_name, job_id, zk, queue=queue, priority=priority)
    finally:
            lock.release()
    return True


def _get_lockpath(typ, app_name, job_id):
    assert typ in set(['execute', 'add'])
    return join(_get_zookeeper_path(app_name, job_id), '%s_lock' % typ)


def _obtain_lock(typ, app_name, job_id, zk,
                 timeout=None, blocking=True, raise_on_error=False, safe=True):
    """Try to acquire a lock.

    `typ` (str) either "execute" or "add"  - defines the type of lock to create
        execute locks guarantee only one instance of a job at a time
        add locks guarantee only one process can queue a job at a time
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

    path = _get_lockpath(typ, app_name, job_id)
    assert path.startswith(_path), "Code Error!"
    l = zk.Lock(path)
    try:
        l.acquire(timeout=timeout, blocking=blocking)
    except:
        if raise_on_error:
            raise exceptions.LockAlreadyAcquired(
                '%s Lock already acquired. %s %s' % (typ, app_name, job_id))
        else:
            log.warn(
                "%s Lock already acquired." % typ,
                extra=dict(app_name=app_name, job_id=job_id))
            return False
    return l


def obtain_add_lock(app_name, job_id, zk, *args, **kwargs):
    """Obtain a lock used when adding a job to queue"""
    return _obtain_lock(
        'add', app_name=app_name, job_id=job_id, zk=zk, *args, **kwargs)


def obtain_execute_lock(app_name, job_id, zk, *args, **kwargs):
    """Obtain a lock used when executing a job"""
    return _obtain_lock(
        'execute', app_name=app_name, job_id=job_id, zk=zk, *args, **kwargs)


def is_execute_locked(app_name, job_id, zk):
    path = _get_lockpath('execute', app_name, job_id)
    try:
        return bool(zk.get_children(path))
    except kazoo.exceptions.NoNodeError:
        return False


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
            'Task retried too many times and is set as permanently failed.',
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
    if pending + completed + failed + skipped != 1:
        raise UserWarning(
            "you must request exactly one state of these options:"
            " pending, completed, failed, skipped")
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
            child_job_id=cjob_id,
            app_name=parent_app_name,
            job_id=parent_job_id)
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

            try:
                readd_subtask(
                    child_app_name, cjob_id, zk=zk,
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


def ensure_parents_completed(app_name, job_id, zk, timeout):
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
        if not check_state(
                app_name=parent, job_id=pjob_id, zk=zk, completed=True):
            parents_completed = False
            log.info(
                'Must wait for parent task to complete before executing'
                ' child task.', extra=dict(
                    parent_app_name=parent, parent_job_id=pjob_id,
                    app_name=app_name, job_id=job_id))
            if maybe_add_subtask(parent, pjob_id, zk):
                msg_if_get_lock = (
                    "Waiting for parent to complete."
                    " Not unqueuing myself"
                    " because parent might have completed already.")
                if _ensure_parents_completed_get_lock(
                        app_name, job_id, parent, pjob_id, zk,
                        timeout, parent_locks, msg_if_get_lock):
                    consume_queue = True
            elif check_state(parent, pjob_id, zk=zk, pending=True):
                msg_if_get_lock = (
                    'A parent is or was previously queued but is not'
                    ' running. Will remove myself from queue since my'
                    ' parent will eventually run and queue me.')
                if _ensure_parents_completed_get_lock(
                        app_name, job_id, parent, pjob_id, zk,
                        timeout, parent_locks, msg_if_get_lock):
                    consume_queue = True
    return parents_completed, consume_queue, parent_locks


def _ensure_parents_completed_get_lock(app_name, job_id, parent, pjob_id, zk,
                                       timeout, parent_locks, msg_if_get_lock):
    """If we can obtain an execute lock on the given parent, then we can
    guarantee that removing the child from its queue is safe"""
    elock = obtain_execute_lock(
        parent, pjob_id, zk=zk,
        raise_on_error=False, timeout=timeout)
    if elock:
        parent_locks.add(elock)
        log.info(msg_if_get_lock, extra=dict(
            parent_app_name=parent, parent_job_id=pjob_id,
            app_name=app_name, job_id=job_id))
        return True
    return False


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
