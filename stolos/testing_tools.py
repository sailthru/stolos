"""
Useful utilities for testing
"""
from contextlib import contextmanager
import os
from os.path import join
import tempfile
import ujson
import nose.tools as nt

from stolos import dag_tools as dt
from stolos.configuration_backend.json_config import JSONMapping
from stolos import zookeeper_tools as zkt


TASKS_JSON = os.environ['TASKS_JSON']
TASKS_JSON_TMPFILES = {}


def configure_logging(logname):
    import logging
    from colorlog import ColoredFormatter

    _ignore_log_keys = set(logging.makeLogRecord({}).__dict__)

    def _json_format(record):
        extras = ' '.join(
            "%s=%s" % (k, record.__dict__[k])
            for k in set(record.__dict__).difference(_ignore_log_keys))
        if extras:
            record.msg = "%s    %s" % (record.msg, extras)
        return record

    class ColoredJsonFormatter(ColoredFormatter):
        def format(self, record):
            record = _json_format(record)
            return super(ColoredJsonFormatter, self).format(record)

    log = logging.getLogger('stolos')
    _formatter = ColoredJsonFormatter(
        "%(log_color)s%(levelname)-8s %(message)s %(reset)s %(cyan)s",
        reset=True)
    if not any(isinstance(h, logging.StreamHandler) for h in log.handlers):
        _h = logging.StreamHandler()
        _h.setFormatter(_formatter)
        log.addHandler(_h)
    log.setLevel(logging.DEBUG)
    log.propagate = False
    return logging.getLogger(logname)


def _smart_run(func, args, kwargs):
    """Given a function definition, determine which of the args and
    kwargs are relevant and then execute the function"""
    fc = func.func_code
    kwargs2 = {
        k: kwargs[k]
        for k in kwargs if k in fc.co_varnames[:fc.co_nlocals]}
    args2 = args[:fc.co_nlocals - len(kwargs2)]
    func(*args2, **kwargs2)


def _with_setup(setup=None, teardown=None, params=False):
    """Decorator to add setup and/or teardown methods to a test function

      @with_setup(setup, teardown)
      def test_something():
          " ... "

    This extends the nose with_setup decorator such that:
        - `setup` can return args and kwargs.
        - The decorated test function and `teardown` can use any of the args
          and kwargs returned by setup
    """
    def decorate(func, setup=setup, teardown=teardown):
        args = []
        kwargs = {}
        if params:
            @nt.make_decorator(func)
            def func_wrapped():
                _smart_run(func, args, kwargs)
        else:
            func_wrapped = func
        if setup:
            def setup_wrapped():
                if params:
                    rv = setup(func.func_name)
                else:
                    rv = setup()
                if rv is not None:
                    args.extend(rv[0])
                    kwargs.update(rv[1])

            if hasattr(func, 'setup'):
                _old_s = func.setup

                def _s():
                    setup_wrapped()
                    _old_s()
                func_wrapped.setup = _s
            else:
                if params:
                    func_wrapped.setup = setup_wrapped
                else:
                    func_wrapped.setup = setup

        if teardown:
            if hasattr(func, 'teardown'):
                _old_t = func.teardown

                def _t():
                    _old_t()
                    _smart_run(teardown, args, kwargs)
                func_wrapped.teardown = _t
            else:
                if params:
                    def _sr():
                        return _smart_run(teardown, args, kwargs)
                    func_wrapped.teardown = _sr
                else:
                    func_wrapped.teardown = teardown
        return func_wrapped
    return decorate


def _create_tasks_json(fname_suffix='', inject={}, rename=False):
    """
    Create a new default tasks.json file
    This of this like a useful setup() operation before tests.

    `inject` - (dct) (re)define new tasks to add to the tasks graph
    mark this copy as the new default.
    `fname_suffix` - suffix in the file name of the new tasks.json file
    `rename` - if True, change the name all tasks to include the fname_suffix

    """
    tasks_config = dt.get_tasks_config().cache
    tasks_config.update(inject)  # assume we're using a json config

    f = tempfile.mkstemp(prefix='tasks_json', suffix=fname_suffix)[1]
    frv = ujson.dumps(tasks_config)
    if rename:
        renames = [(k, "%s__%s" % (k, fname_suffix))
                   for k in tasks_config]
        for k, new_k in renames:
            frv = frv.replace(ujson.dumps(k), ujson.dumps(new_k))
    with open(f, 'w') as fout:
        fout.write(frv)

    os.environ['TASKS_JSON'] = f
    try:
        dt.build_dag_cached.cache.clear()
    except AttributeError:
        pass
    return f


def _setup_func(func_name):
    """Code that runs just before each test and configures a tasks.json file
    for each test.  The tasks.json tmp files are stored in a global var."""
    zk = zkt.get_zkclient('localhost:2181')
    zk.delete('test_stolos', recursive=True)

    rv = dict(
        zk=zk,
        oapp1='test_stolos/test_app',
        oapp2='test_stolos/test_app2',
        app3='test_stolos/test_app3__%s' % func_name,
        app4='test_stolos/test_app4__%s' % func_name,
        job_id1='20140606_1111_profile',
        job_id2='20140606_2222_profile',
        job_id3='20140604_1111_profile',
        depends_on1='test_stolos/test_depends_on__%s' % func_name,
        bash1='test_stolos/test_bash__%s' % func_name,
        func_name=func_name,
    )
    rv.update(
        app1='%s__%s' % (rv['oapp1'], func_name),
        app2='%s__%s' % (rv['oapp2'], func_name),
    )

    f = _create_tasks_json(fname_suffix=func_name, rename=True)
    TASKS_JSON_TMPFILES[func_name] = f
    return ((), rv)


def _teardown_func(func_name):
    """Code that runs just after each test"""
    os.environ['TASKS_JSON'] = TASKS_JSON
    os.remove(TASKS_JSON_TMPFILES[func_name])


def with_setup(func, setup_func=_setup_func, teardown_func=_teardown_func):
    """Decorator that wraps a test function and provides setup() and teardown()
    functionality.  The decorated func may define as a parameter any kwargs
    returned by setup(func_name).  The setup() is func.name aware but not
    necessarily module aware, so be careful if using the same test function in
    different test modules
    """
    return _with_setup(setup_func, teardown_func, True)(func)


@contextmanager
def inject_into_dag(new_task_dct):
    """Update (add or replace) tasks in dag with new task config.
    This should reset any cacheing within the running Stolos instance,
    but it's not guaranteed.
    Assumes that the config we're using is the JSONMapping
    """
    f = _create_tasks_json(inject=new_task_dct)
    new_task_dct = JSONMapping(new_task_dct)

    # verify injection worked
    dg = dt.get_tasks_config()
    assert isinstance(dg, JSONMapping)
    dag = dt.build_dag_cached()
    for k, v in new_task_dct.items():
        assert dg[k] == v, (
            "test code: inject_into_dag didn't insert the new tasks?")
        assert dag.node[k] == dict(v), (
            "test code: inject_into_dag didn't reset the dag graph")

    yield
    os.remove(f)
    dt.build_dag_cached.cache.clear()


def enqueue(app_name, job_id, zk, validate_queued=True):
    # initialize job
    zkt.maybe_add_subtask(app_name, job_id, zk)
    zkt.maybe_add_subtask(app_name, job_id, zk)
    # verify initial conditions
    if validate_queued:
        validate_one_queued_task(zk, app_name, job_id)


def validate_zero_queued_task(zk, app_name):
    if zk.exists(join(app_name, 'entries')):
        nt.assert_equal(
            0, len(zk.get_children(join(app_name, 'entries'))))


def validate_zero_completed_task(zk, app_name):
    if zk.exists(join(app_name, 'all_subtasks')):
        nt.assert_equal(
            0, len(zk.get_children(join(app_name, 'all_subtasks'))))


def validate_one_failed_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    # nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'failed')


def validate_one_queued_executing_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 1)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], True)
    nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'pending')


def validate_one_queued_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], True)
    nt.assert_equal(status['app_qsize'], 1)
    nt.assert_equal(status['state'], 'pending')


def validate_one_completed_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'completed')


def validate_one_skipped_task(zk, app_name, job_id):
    status = get_zk_status(zk, app_name, job_id)
    nt.assert_equal(status['num_execute_locks'], 0)
    nt.assert_equal(status['num_add_locks'], 0)
    nt.assert_equal(status['in_queue'], False)
    nt.assert_equal(status['app_qsize'], 0)
    nt.assert_equal(status['state'], 'skipped')


def validate_two_queued_task(zk, app_name, job_id1, job_id2):
    for job_id in [job_id1, job_id2]:
        status = get_zk_status(zk, app_name, job_id)
        nt.assert_equal(status['num_execute_locks'], 0)
        nt.assert_equal(status['num_add_locks'], 0)
        nt.assert_equal(status['in_queue'], True)
        nt.assert_equal(status['app_qsize'], 2)
        nt.assert_equal(status['state'], 'pending')


def cycle_queue(zk, app_name):
    """Get item from queue, put at back of queue and return item"""
    q = zk.LockingQueue(app_name)
    item = q.get()
    q.put(item)
    q.consume()
    return item


def consume_queue(zk, app_name, timeout=1):
    q = zk.LockingQueue(app_name)
    item = q.get(timeout=timeout)
    q.consume()
    return item


def get_zk_status(zk, app_name, job_id):
    path = zkt._get_zookeeper_path(app_name, job_id)
    elockpath = join(path, 'execute_lock')
    alockpath = join(path, 'add_lock')
    entriespath = join(app_name, 'entries')
    return {
        'num_add_locks': len(
            zk.exists(alockpath) and zk.get_children(alockpath) or []),
        'num_execute_locks': len(
            zk.exists(elockpath) and zk.get_children(elockpath) or []),
        'in_queue': (
            any(zk.get(join(app_name, 'entries', x))[0] == job_id
                for x in zk.get_children(entriespath))
            if zk.exists(entriespath) else False),
        'app_qsize': (
            len(zk.get_children(entriespath))
            if zk.exists(entriespath) else 0),
        'state': zk.get(path)[0],
    }
