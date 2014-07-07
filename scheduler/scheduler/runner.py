import importlib

from ds_commons import argparse_tools as at
from ds_commons.log import log
from scheduler import dag_tools, exceptions, zookeeper_tools


def main(ns):
    """Initialize a worker to fetch a job from a given application's queue
    Execute the given code, and then gracefully die
    """
    zk = zookeeper_tools.get_client(ns.zookeeper_hosts)
    q = zk.LockingQueue(ns.app_name)
    job_id = q.get(timeout=ns.timeout)
    if job_id is None:
        log.info('No jobs found in %d seconds...' % ns.timeout)
        return

    if not ensure_parents_completed(
            app_name=ns.app_name, job_id=job_id, zk=zk, q=q):
        return

    lock = get_lock_if_job_is_runnable(
        app_name=ns.app_name, job_id=job_id, zk=zk)
    if lock is False:
        # infinite loop: some jobs will always requeue if lock is unobtainable
        _send_to_back_of_queue(q=q, app_name=ns.app_name, job_id=job_id, zk=zk)
        return

    try:
        ns.job_type_func(ns=ns, job_id=job_id)
    except exceptions.CodeError:  # assume error is previously logged
        _handle_failure(ns, job_id, zk, q, lock)
        return
    except Exception as err:
        log.exception(
            ("Shit!  Unhandled exception in an application! Fix ASAP because"
             " it is unclear how to handle this failure.  %s: %s") % (
                err.__class__.__name__, err),
            extra=ns.__dict__)
        return
    _handle_success(ns, job_id, zk, q, lock)


def get_lock_if_job_is_runnable(app_name, job_id, zk):
    """Return a lock instance or False.  If returning False,
    the job is not ready to execute"""

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
    l = zookeeper_tools.obtain_lock(app_name, job_id, zk)
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


def ensure_parents_completed(app_name, job_id, zk, q):
    """
    Check that the parent tasks for this (app_name, job_id) pair have completed
    If they haven't completed and aren't pending, maybe create the
    parent task in its appropriate queue.
    """
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
            zookeeper_tools.maybe_add_subtask(parent, pjob_id, zk)
            q.consume()
            return False

    return True


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


def build_worker_arg_parser(*args, **kwargs):
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
        '--timeout', default=2, type=int,
        help='time to wait for task to appear in queue before dying'),
    at.add_argument(
        '--max_retry', type=int, default=5,
        help='Maximum number of times to retry a failed task.'),
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
    Lazily import the appropriate worker module based on the given app name
    And recreate the namespace with arguments specific to that worker module
    """
    ns, _ = _build_arg_parser().parse_known_args()
    worker = importlib.import_module(
        'scheduler.worker.%s_worker' % dag_tools.get_job_type(ns.app_name))
    ns = worker.build_arg_parser().parse_args()
    ns.job_type_func = worker.main
    return ns


if __name__ == '__main__':
    NS = build_arg_parser()
    main(NS)
