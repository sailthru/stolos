from stolos import get_NS
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
    qb = get_NS().queue_backend()
    _path = shared.get_job_path(app_name, job_id)
    if safe and not qb.exists(_path):
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
    l = qb.Lock(path)
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
    qb = get_NS().queue_backend()
    path = shared.get_lock_path('execute', app_name, job_id)
    try:
        return bool(qb.get_children(path))
    except exceptions.NoNodeError:
        return False
