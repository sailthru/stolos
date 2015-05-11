from stolos import exceptions

from . import log
from . import shared


def obtain_add_lock(app_name, job_id, *args, **kwargs):
    """Obtain a lock used when adding a job to queue"""
    return _obtain_lock(
        'add', app_name=app_name, job_id=job_id, *args, **kwargs)


def obtain_execute_lock(app_name, job_id, *args, **kwargs):
    """Obtain a lock used when executing a job"""
    return _obtain_lock(
        'execute', app_name=app_name, job_id=job_id, *args, **kwargs)


def _obtain_lock(typ, app_name, job_id, timeout=None,
                 blocking=False, raise_on_error=False, safe=True):
    """Try to acquire a lock.

    `typ` (str) either "execute" or "add"  - defines the type of lock to create
        execute locks guarantee only one instance of a job at a time
        add locks guarantee only one process can queue a job at a time
    `safe` (bool) By default, this function is `safe` because it will not
        create a node for the job_id if it doesn't already exist.
    `raise_on_error` (bool) if False, just return False.
        Otherwise, raise errors if LockAlreadyAcquired or if safe=True
        and job_id path does not exist
    `timeout` (int) num seconds to wait client-side to try to acquire lock
    `blocking` (bool) if True, wait up to `timeout` to acquire lock, or forever
        if False, return as soon as we confirm that anyone has lock

    Either return a lock or [ raise | return False ]
    """
    qbcli = shared.get_qbclient()
    _path = shared.get_job_path(app_name, job_id)
    if safe and not qbcli.exists(_path):
        log.warn(
            "Cannot create a lock if the task hasn't been added yet!",
            extra=dict(app_name=app_name, job_id=job_id))
        if raise_on_error:
            raise exceptions.CouldNotObtainLock(
                'You must create a job_id before obtaining a lock on it!'
                ' %s %s' % (app_name, job_id))
        else:
            return False

    path = shared.get_lock_path(typ, app_name, job_id)
    assert path.startswith(_path), "Code Error!"
    l = qbcli.Lock(path)
    if not l.acquire(timeout=timeout, blocking=blocking):
        if raise_on_error:  # TODO: rename to raise_if_acquired
            raise exceptions.LockAlreadyAcquired(
                '%s Lock already acquired. %s %s' % (typ, app_name, job_id))
        else:
            log.debug(
                "%s Lock already acquired." % typ,
                extra=dict(app_name=app_name, job_id=job_id))
            return False
    return l


def is_execute_locked(app_name, job_id):
    """Does anyone currently have the execute lock?"""
    qbcli = shared.get_qbclient()
    path = shared.get_lock_path('execute', app_name, job_id)
    return qbcli.Lock(path).is_locked()
