import importlib

from scheduler import argparse_shared as at
from scheduler import log
from scheduler import dag_tools, exceptions, zookeeper_tools


def main(ns):
    """
    Fetch a job from a given application's queue and obtain ZooKeeper lock(s),
    Execute the given code using appropriate plugin module,
    Handle dependencies
    And then gracefully die
    """
    if ns.bypass_scheduler:
        log.info(
            "Running a task without scheduling anything"
            " or fetching from a queue", extra=dict(
                app_name=ns.app_name, job_id=ns.job_id))
        assert ns.job_id
        ns.job_type_func(ns=ns)
        return

    log.info("Beginning Scheduler", extra=dict(**ns.__dict__))
    zk = zookeeper_tools.get_client(ns.zookeeper_hosts)
    q = zk.LockingQueue(ns.app_name)
    if ns.job_id:
        lock = _handle_manually_given_job_id(ns, zk)
    else:
        lock = None
        ns.job_id = q.get(timeout=ns.timeout)
        if not validate_job_id(app_name=ns.app_name, job_id=ns.job_id,
                               q=q, zk=zk, timeout=ns.timeout):
            return
    lock = get_lock_if_job_is_runnable(
        app_name=ns.app_name, job_id=ns.job_id, zk=zk, timeout=ns.timeout,
        lock=lock)

    if lock is False:
        # infinite loop: some jobs will always requeue if lock is unobtainable
        log.info("Could not obtain a lock.  Will requeue and try again later",
                 extra=dict(app_name=ns.app_name, job_id=ns.job_id))
        _send_to_back_of_queue(
            q=q, app_name=ns.app_name, job_id=ns.job_id, zk=zk)
        return

    parents_completed, queued_parent = ensure_parents_completed(
        app_name=ns.app_name, job_id=ns.job_id, zk=zk)
    if not parents_completed:
        if queued_parent:
            q.consume()
        else:
            _send_to_back_of_queue(
                q=q, app_name=ns.app_name, job_id=ns.job_id, zk=zk)
        lock.release()
        return

    try:
        ns.job_type_func(ns=ns)
    except exceptions.CodeError:  # assume error is previously logged
        _handle_failure(ns, ns.job_id, zk, q, lock)
        return
    except Exception as err:
        log.exception(
            ("Shit!  Unhandled exception in an application! Fix ASAP because"
             " it is unclear how to handle this failure. Job failure.  %s: %s")
            % (err.__class__.__name__, err), extra=ns.__dict__)
        return
    _handle_success(ns, ns.job_id, zk, q, lock)


def validate_job_id(app_name, job_id, q, zk, timeout):
    """Return True if valid job_id.
    If invalid, do whatever cleanup for this job is necessary and return False.
      --> necessary cleanup may include removing this job_id from queue
    """
    if job_id is None:
        log.info('No jobs found in %d seconds...' % timeout)
        return False
    try:
        dag_tools.parse_job_id(app_name, job_id)
    except exceptions.InvalidJobId as err:
        log.error((
            "Scheduler found an invalid job_id.  Removing it from queue"
            " and marking that job_id as failed.  Error details: %s") % err,
            extra=dict(app_name=app_name, job_id=job_id))
        q.consume()
        zookeeper_tools._set_state_unsafe(
            app_name, job_id, zk=zk, failed=True)
        return False
    return True


def _handle_manually_given_job_id(ns, zk):
    """This process was given a specific --job_id arg.
    Decide whether it's okay to execute this job_id,
    and if its okay to go forward, set job_id state appropriately
    """
    log.warn(
        ('using specific job_id and'
         ' blindly assuming this job is not already queued.'),
        extra=dict(app_name=ns.app_name, job_id=ns.job_id))
    created = zk.exists(zookeeper_tools._get_zookeeper_path(
        ns.app_name, ns.job_id))
    if created:
        msg = ('Will not execute this task because it might be already'
               ' queued or completed!')
        log.critical(
            msg, extra=dict(app_name=ns.app_name, job_id=ns.job_id))
        raise UserWarning(msg)
    lock = zookeeper_tools.obtain_lock(
        ns.app_name, ns.job_id, zk=zk, safe=False, raise_on_error=True,
        timeout=ns.timeout)
    zookeeper_tools.set_state(ns.app_name, ns.job_id, zk=zk, pending=True)
    return lock


def get_lock_if_job_is_runnable(app_name, job_id, zk, timeout, lock):
    """Return a lock instance or False.  If returning False,
    the job is not ready to execute.  If we already have the lock, use that one
    """

    available = zookeeper_tools.check_state(
        app_name, job_id, zk, pending=True, raise_if_not_exists=True)
    if not available:
        try:
            raise RuntimeError(
                "I found a job in queue that wasn't"
                " in state pending. This might be a code bug. You"
                " probably queued 2+ of the same job!")
        except RuntimeError as err:
            # force a traceback in the logs
            log.exception(
                err, extra=dict(
                    app_name=app_name,
                    job_id=job_id,
                    state=zookeeper_tools.check_state(
                        app_name, job_id, zk=zk, _get=True)))
            return False
    if lock:
        l = lock
    else:
        l = zookeeper_tools.obtain_lock(app_name, job_id, zk, timeout=timeout)
    if l is False:
        log.warn('Could not obtain lock for task most likely because'
                 ' the job_id appears more than once in the queue',
                 extra=dict(app_name=app_name, job_id=job_id))
        return False
    return l


def _send_to_back_of_queue(q, app_name, job_id, zk):
    # this exists so un-runnable tasks don't hog the front of the queue
    # and soak up resources
    q.put(job_id)
    q.consume()
    log.info(
        "Job sent to back of queue",
        extra=dict(app_name=app_name, job_id=job_id))


def ensure_parents_completed(app_name, job_id, zk):
    """
    Check that the parent tasks for this (app_name, job_id) pair have completed
    If they haven't completed and aren't pending, maybe create the
    parent task in its appropriate queue.
    """
    parents_completed = True
    queued_parent = False
    for parent, pjob_id, dep_grp in dag_tools.get_parents(app_name,
                                                          job_id, True):
        if not zookeeper_tools.check_state(
                app_name=parent, job_id=pjob_id, zk=zk, completed=True):
            log.info(
                'Must wait for parent task to complete before executing'
                ' child task. Removing job from queue.  It will get re-added'
                ' when parent tasks complete', extra=dict(
                    parent_app_name=parent, parent_job_id=pjob_id,
                    child_app_name=app_name, child_job_id=job_id))
            if zookeeper_tools.maybe_add_subtask(parent, pjob_id, zk):
                queued_parent = True
            parents_completed = False
    return parents_completed, queued_parent


def _handle_failure(ns, job_id, zk, q, lock):
    """The job has failed.  Increase it's retry limit, send to back of queue,
    and release the lock"""
    exceeded_retry_limit = zookeeper_tools.inc_retry_count(
        app_name=ns.app_name, job_id=job_id, zk=zk,
        max_retry=ns.max_retry)
    if exceeded_retry_limit:
        q.consume()
    else:
        _send_to_back_of_queue(
            q=q, app_name=ns.app_name, job_id=job_id, zk=zk)
    lock.release()
    log.warn(
        "Job failed", extra=dict(job_id=job_id, app_name=ns.app_name))


def _handle_success(ns, job_id, zk, q, lock):
    zookeeper_tools.set_state(
        app_name=ns.app_name, job_id=job_id, zk=zk, completed=True)
    q.consume()
    lock.release()
    log.info(
        "successfully completed job",
        extra=dict(app_name=ns.app_name, job_id=job_id))


def log_and_raise(err, log_details):
    """A helper function that logs the given exception
    and then raises a generic CodeError.  This is useful to guarantee
    that failing jobs are properly handled
    """
    log.exception(err, extra=log_details)
    raise exceptions.CodeError(
        'Task failed.  This message should never appear in logs.')


def build_plugin_arg_parser(*args, **kwargs):
    return at.build_arg_parser(
        *args,
        parents=kwargs.pop('parents', [_build_arg_parser()]),
        conflict_handler='resolve',
        add_help=kwargs.pop('add_help', True),
        **kwargs)


_build_arg_parser = at.build_arg_parser([
    at.app_name(required=True, choices=dag_tools.get_task_names()),
    at.zookeeper_hosts,

    at.add_argument(
        '--bypass_scheduler', action='store_true',
        help=(
            "Run a task directly. Do not schedule it."
            "  Do not obtain a lock on this job.  Requires passing --job_id")),
    at.add_argument(
        '--timeout', default=2, type=int,
        help='time to wait for task to appear in queue before dying'),
    at.add_argument(
        '--max_retry', type=int, default=5,
        help='Maximum number of times to retry a failed task.'),
    at.add_argument(
        '--job_id', help=(
            'run a specific job_id. If a job is already queued,'
            ' it will run twice')),
],
    description=(
        "A wrapper script that fetches tasks from a particular application's"
        " queue, executes the task and then dies.  Jobs are managed in a DAG"
        " with ZooKeeper"),
    add_help=False  # this parser option is overridden by child parsers
)


def build_arg_parser():
    """
    Get an argparse.Namespace from sys.argv,
    Lazily import the appropriate plugin module based on the given app name
    And recreate the namespace with arguments specific to that plugin module
    """
    ns, _ = _build_arg_parser().parse_known_args()
    plugin = importlib.import_module(
        'scheduler.plugins.%s_plugin' % dag_tools.get_job_type(ns.app_name))
    ns = plugin.build_arg_parser().parse_args()
    ns.job_type_func = plugin.main
    return ns


if __name__ == '__main__':
    NS = build_arg_parser()
    main(NS)
