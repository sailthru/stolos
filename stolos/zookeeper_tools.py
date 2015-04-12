"""
Manages the application queues in zookeeper
"""
import atexit
from kazoo.client import KazooClient
import kazoo.exceptions
from os.path import join

import stolos
from stolos import util
from stolos import log
from stolos import dag_tools
from stolos import exceptions

from . import task_states

@util.cached
def get_zkclient():
    """Start and return a connection to ZooKeeper"""
    zookeeper_hosts = stolos.get_NS().zookeeper_hosts
    log.debug(
        "Connecting to ZooKeeper", extra=dict(zookeeper_hosts=zookeeper_hosts))
    zk = KazooClient(zookeeper_hosts)
    zk.logger.handlers = log.handlers
    zk.logger.setLevel('WARN')
    zk.start()
    atexit.register(zk.stop)
    return zk


def get_qsize(app_name, queued=True, taken=True):
    """
    Find the number of jobs in the given app's queue

    `queued` - Include the entries in the queue that are not currently
        being processed or otherwise locked
    `taken` - Include the entries in the queue that are currently being
        processed or are otherwise locked
    """
    zk = get_zkclient()
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


def _queue(app_name, job_id, queue=True, priority=None):
    """ Calling code should obtain a lock first!
    If queue=False, do everything except queue (ie set state)"""
    zk = get_zkclient()
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
        set_state(app_name, job_id, pending=True)
    else:
        log.info(
            'job invalid.  marking as skipped so it does not run',
            extra=dict(app_name=app_name, job_id=job_id))
        set_state(app_name, job_id, skipped=True)


def check_if_queued(app_name, job_id):
    zk = get_zkclient()
    p = join(app_name, 'entries')
    try:
        queued_jobs = {zk.get(join(p, x))[0] for x in zk.get_children(p)}
    except kazoo.exceptions.NoNodeError:
        queued_jobs = set()

    if job_id in queued_jobs:
        return True
    return False


@util.pre_condition(dag_tools.parse_job_id)
def maybe_add_subtask(app_name, job_id, timeout=5, queue=True, priority=None):
    """Add a subtask to the queue if it hasn't been added yet.

    `queue` (bool, optional) - if False, don't add the subtask to queue
    `timeout` (int, optional) - num seconds to wait for a lock before queueing
    `priority` (int, optional) - prioritize this item in the queue.
        1 is highest priority 100 is lowest priority.
        Irrelevant if `queue` is False
    """
    zk = get_zkclient()
    if zk.exists(get_job_path(app_name, job_id)):
        return False
    # get a lock so we guarantee this task isn't being added twice concurrently
    lock = obtain_add_lock(app_name, job_id, timeout=timeout, safe=False)
    if not lock:
        return False
    try:
        _queue(app_name, job_id, queue=queue, priority=priority)
    finally:
            lock.release()
    return True


def _obtain_lock(typ, app_name, job_id,
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
    zk = get_zkclient()
    _path = get_job_path(app_name, job_id)
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


def is_execute_locked(app_name, job_id):
    zk = get_zkclient()
    path = _get_lockpath('execute', app_name, job_id)
    try:
        return bool(zk.get_children(path))
    except kazoo.exceptions.NoNodeError:
        return False


@util.pre_condition(dag_tools.parse_job_id)
def inc_retry_count(app_name, job_id, max_retry):
    """Increment the retry count for the given task.  If the retry count is
    greater than the max allowed number of retries, set the tasks's state
    to failed.

    Returns False if task exceeded retry limit and True if the increment was
    fine
    """
    zk = get_zkclient()
    path = join(get_job_path(app_name, job_id), 'retry_count')
    if not zk.exists(path):
        zk.create(path, '0', makepath=False)
        cnt = 0
    else:
        cnt = int(zk.get(path)[0])
    if cnt + 1 >= max_retry:
        set_state(app_name, job_id, failed=True)
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


def _recursively_reset_child_task_state(parent_app_name, job_id):
    zk = get_zkclient()
    log.debug(
        "recursively setting all descendant tasks to 'pending' and "
        " marking that the parent is not completed",
        extra=dict(app_name=parent_app_name, job_id=job_id))

    gen = dag_tools.get_children(parent_app_name, job_id, True)
    for child_app_name, cjob_id, dep_grp in gen:
        child_path = get_job_path(child_app_name, cjob_id)
        if zk.exists(child_path):
            set_state(child_app_name, cjob_id, pending=True)
            _recursively_reset_child_task_state(child_app_name, cjob_id)
        else:
            pass  # no need to recurse further down the tree


def _set_state_unsafe(
        app_name, job_id,
        pending=False, completed=False, failed=False, skipped=False):
    """
    Set the state of a task

    `app_name` is a task identifier
    `job_id` is a subtask identifier
    `pending`, `completed` and `failed` (bool) are mutually exclusive
    """
    zk = get_zkclient()
    zookeeper_path = get_job_path(app_name, job_id)
    state = task_states.validate_state(pending, completed, failed, skipped)
    if completed:  # basecase
        _maybe_queue_children(parent_app_name=app_name, parent_job_id=job_id)

    if zk.exists(zookeeper_path):
        zk.set(zookeeper_path, state)
    else:
        zk.create(zookeeper_path, state, makepath=True)

    log.debug(
        "Set task state",
        extra=dict(state=state, app_name=app_name, job_id=job_id))

set_state = util.pre_condition(dag_tools.parse_job_id)(
    _set_state_unsafe)


def check_state(app_name, job_id, raise_if_not_exists=False,
                pending=False, completed=False, failed=False, skipped=False,
                all=False, _get=False):
    """Determine whether a specific job is in one or more specific state(s)

    If job_id is a string, return a single value.
    If multiple job_ids are given, return a list of values

    `app_name` is a task identifier
    `job_id` (str or list of str) is a subtask identifier or a list of them
    `all` (bool) if True, return True if the job_id is in a recognizable state
    `_get` (bool) if True, just return the string value of the state and
                  ignore the (pending, completed, xor failed) choice
    """
    zk = get_zkclient()
    if isinstance(job_id, (str, unicode)):
        job_ids = [job_id]
        rvaslist = False
    else:
        job_ids = job_id
        rvaslist = True

    rv = []
    for job_id in job_ids:
        zookeeper_path = get_job_path(app_name, job_id)
        try:
            gotstate = zk.get(zookeeper_path)[0]
        except kazoo.exceptions.NoNodeError:
            if raise_if_not_exists:
                raise
            else:
                rv.append(False)
                continue
        if _get:
            rv.append(gotstate)
            continue
        else:
            accepted_states = task_states.validate_state(
                pending, completed, failed, skipped, all=all, multi=True)
            rv.append(gotstate in accepted_states)
            continue
    if rvaslist:
        return rv
    else:
        return rv[0]
