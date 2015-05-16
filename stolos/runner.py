"""
This code fetches jobs from the queue, decides whether to run jobs, and then
runs them or manipulates its own and parent/child queues
"""
import importlib

from stolos import argparse_shared as at
from stolos import log
from stolos import dag_tools as dt, exceptions
from stolos import queue_backend as qb
from stolos import configuration_backend as cb
from stolos.initializer import initialize


def main(ns):
    """
    Fetch a job_id from the `app_name` queue and figure out what to with it.

    If the job is runnable, execute it and then queue its children into their
    respective queues.  If it's not runnable, queue its parents into respective
    parent queues and remove the job from its own queue.
    If the job fails, either requeue it or mark it as permanently failed
    """
    assert ns.app_name in dt.get_task_names()
    if ns.bypass_scheduler:
        log.info(
            "Running a task without scheduling anything"
            " or fetching from a queue", extra=dict(
                app_name=ns.app_name, job_id=ns.job_id))
        assert ns.job_id
        ns.job_type_func(ns=ns)
        return

    log.info("Beginning Stolos", extra=dict(**ns.__dict__))
    q = qb.get_qbclient().LockingQueue(ns.app_name)
    if ns.job_id:
        lock = _handle_manually_given_job_id(ns)
        q.consume = object  # do nothing
    else:
        ns.job_id = q.get(timeout=ns.timeout)
        if not validate_job_id(app_name=ns.app_name, job_id=ns.job_id,
                               q=q, timeout=ns.timeout):
            return
        try:
            lock = get_lock_if_job_is_runnable(
                app_name=ns.app_name, job_id=ns.job_id)
        except exceptions.NoNodeError:
            q.consume()
            log.exception(
                "Job failed. The job is queued, so why does its state not"
                " exist?  The Queue backend may be in an inconsistent state."
                " Consuming this job",
                extra=dict(app_name=ns.app_name, job_id=ns.job_id))
            return

    log.debug(
        "Stolos got a job_id.", extra=dict(
            app_name=ns.app_name, job_id=ns.job_id, acquired_lock=bool(lock)))
    if lock is False:
        # infinite loop: some jobs will always requeue if lock is unobtainable
        log.info("Could not obtain a lock.  Will requeue and try again later",
                 extra=dict(app_name=ns.app_name, job_id=ns.job_id))
        _send_to_back_of_queue(
            q=q, app_name=ns.app_name, job_id=ns.job_id)
        return

    if not parents_completed(ns.app_name, ns.job_id, q=q, lock=lock):
        return

    log.info(
        "Job starting!", extra=dict(app_name=ns.app_name, job_id=ns.job_id))
    try:
        ns.job_type_func(ns=ns)
    except exceptions.CodeError:  # assume error is previously logged
        _handle_failure(ns, q, lock)
        return
    except Exception as err:
        log.exception(
            ("Job failed!  Unhandled exception in an application!"
             " Fix ASAP because"
             " it is unclear how to handle this failure.  %s: %s")
            % (err.__class__.__name__, err), extra=dict(
                app_name=ns.app_name, job_id=ns.job_id, failed=True))
        return
    _handle_success(ns, q, lock)


def parents_completed(app_name, job_id, q, lock):
    """Ensure parents are completed and if they aren't, return False"""
    parents_completed, consume_queue, parent_lock = \
        qb.ensure_parents_completed(app_name=app_name, job_id=job_id)
    if parents_completed is False:
        if consume_queue:
            q.consume()
            parent_lock.release()
        else:
            _send_to_back_of_queue(
                q=q, app_name=app_name, job_id=job_id)
        lock.release()
        return False
    else:
        assert parent_lock is None, "Code Error"
        return True


def validate_job_id(app_name, job_id, q, timeout):
    """Return True if valid job_id.
    If invalid, do whatever cleanup for this job is necessary and return False.
      --> necessary cleanup may include removing this job_id from queue
    """
    if job_id is None:
        log.info('No jobs found in %d seconds...' % timeout, extra=dict(
            app_name=app_name))
        return False
    try:
        dt.parse_job_id(app_name, job_id)
    except exceptions.InvalidJobId as err:
        log.error((
            "Stolos found an invalid job_id.  Removing it from queue"
            " and marking that job_id as failed.  Error details: %s") % err,
            extra=dict(app_name=app_name, job_id=job_id))
        q.consume()
        qb._set_state_unsafe(
            app_name, job_id, failed=True)
        return False
    return True


def _handle_manually_given_job_id(ns):
    """This process was given a specific --job_id arg.
    Decide whether it's okay to execute this job_id,
    and if its okay to go forward, set job_id state appropriately
    """
    log.warn(
        ('using specific job_id and'
         ' blindly assuming this job is not already queued.'),
        extra=dict(app_name=ns.app_name, job_id=ns.job_id))
    if qb.get_qbclient().exists(qb.get_job_path(ns.app_name, ns.job_id)):
        msg = ('Will not execute this task because it might be already'
               ' queued or completed!')
        log.critical(
            msg, extra=dict(app_name=ns.app_name, job_id=ns.job_id))
        raise UserWarning(msg)
    lock = qb.obtain_execute_lock(
        ns.app_name, ns.job_id, safe=False, raise_on_error=True,
        blocking=False)
    qb.set_state(ns.app_name, ns.job_id, pending=True)
    return lock


def get_lock_if_job_is_runnable(app_name, job_id):
    """Return a lock instance or False.  If returning False,
    the job is not ready to execute.
    """

    available = qb.check_state(
        app_name, job_id, pending=True, raise_if_not_exists=True)
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
                    state=qb.check_state(
                        app_name, job_id, _get=True)))
            return False
    l = qb.obtain_execute_lock(app_name, job_id, blocking=False)
    if l is False:
        log.warn('Could not obtain execute lock for task because'
                 ' something is already processing this job_id',
                 extra=dict(app_name=app_name, job_id=job_id))
        return False
    return l


def _send_to_back_of_queue(q, app_name, job_id):
    # this exists so un-runnable tasks don't hog the front of the queue
    # and soak up resources
    try:
        qb.readd_subtask(app_name, job_id, _force=True)
        q.consume()
        log.info(
            "Job sent to back of queue",
            extra=dict(app_name=app_name, job_id=job_id))
    except exceptions.JobAlreadyQueued:
        log.info(
            "Job already queued. Cannot send to back of queue.",
            extra=dict(app_name=app_name, job_id=job_id))


def _handle_failure(ns, q, lock):
    """The job has failed.  Increase it's retry limit, send to back of queue,
    and release the lock"""
    exceeded_retry_limit = qb.inc_retry_count(
        app_name=ns.app_name, job_id=ns.job_id, max_retry=ns.max_retry)
    if exceeded_retry_limit:
        q.consume()
    else:
        _send_to_back_of_queue(
            q=q, app_name=ns.app_name, job_id=ns.job_id)
    if lock:
        lock.release()
    log.warn("Job failed", extra=dict(
        job_id=ns.job_id, app_name=ns.app_name, failed=True))


def _handle_success(ns, q, lock):
    qb.set_state(
        app_name=ns.app_name, job_id=ns.job_id, completed=True)
    q.consume()
    lock.release()
    log.info(
        "successfully completed job",
        extra=dict(app_name=ns.app_name, job_id=ns.job_id, completed=True))


def build_arg_parser_and_parse_args():
    # """
    # Get an argparse.Namespace from sys.argv,
    # Lazily import the appropriate plugin module based on the given app name
    # And recreate the namespace with arguments specific to that plugin module
    # """

    parser = at.build_arg_parser([at.group(
        "Runtime options",
        at.app_name,
        at.add_argument(
            '--bypass_scheduler', action='store_true', help=(
                "Run a task directly. Do not schedule it."
                "  Do not obtain a lock on this job."
                "  Requires passing --job_id")),
        at.add_argument(
            '--timeout', type=int, default=2,
            help='time to wait for task to appear in queue before dying'),
        at.add_argument(
            '--max_retry', type=int, default=5,
            help='Maximum number of times to retry a failed task.'),
        at.add_argument(
            '--job_id', help=(
                'run a specific job_id. If a job is already queued,'
                ' it will run twice')),
    )], description=(
        "This script intelligently executes your application's jobs."
        " Specifically, an instance of this script fetches exactly 1 job"
        " from your application's queue, decides how to perform those jobs,"
        " and then dies.  Because jobs are managed in a DAG, Stolos may choose"
        " to delay execution of a job until dependencies have been met."
        " It may also queue child or parent jobs depending on their status."),
    )
    parser, ns = initialize(
        [parser(), dt, cb, qb],
        parse_known_args=True)

    # get plugin parser
    plugin = importlib.import_module(
        'stolos.plugins.%s_plugin' % dt.get_job_type(ns.app_name))
    ns = at.build_arg_parser(
        parents=[parser, plugin.build_arg_parser()],
        add_help=True
    ).parse_args()
    ns.job_type_func = plugin.main
    return ns


if __name__ == '__main__':
    NS = build_arg_parser_and_parse_args()
    main(NS)
