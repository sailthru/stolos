from contextlib import contextmanager
import logging
import os
from os.path import join, abspath, dirname
import simplejson
import tempfile

from stolos.initializer import initialize as _initialize
from stolos import util

from stolos import queue_backend as qb
from stolos import dag_tools as dt
from stolos import configuration_backend as cb

from .with_setup_tools import with_setup, smart_run

TASKS_JSON_READ_FP = join(
    dirname(dirname(abspath(__file__))), 'examples/tasks.json')


def makepath(func_name, k=None):
    if k is not None:
        return 'test_stolos/%s/%s' % (func_name, k)
    return 'test_stolos/%s' % func_name


def _create_tasks_json(func_name, inject={}):
    """
    Create a new default tasks.json file
    This of this like a useful setup() operation before tests.

    `inject` - (dct) (re)define new tasks to add to the tasks graph
    mark this copy as the new default.
    `func_name` - the name of the function we're testing

    """
    tasks_config = simplejson.load(open(TASKS_JSON_READ_FP))

    # change the name of all tasks to include the func_name
    tc1 = simplejson.dumps(tasks_config)
    tc2 = simplejson.dumps(inject)
    renames = [
        (k, makepath(func_name, k))
        for k in (x for y in (tasks_config, inject) for x in y)
        if not k.startswith(makepath(func_name))]
    for k, new_k in renames:
        tc1 = tc1.replace(simplejson.dumps(k), simplejson.dumps(new_k))
        tc2 = tc2.replace(simplejson.dumps(k), simplejson.dumps(new_k))
    # hacky: change the job_ids that may be mentioned in tasks.json
    hack_renames = [
        (x, '%s-%s' % (x, func_name)) for x in
        ['profile', 'purchase'] + ['testID%s' % i for i in range(1, 7)]]
    for k, new_k in hack_renames:
        tc1 = tc1.replace(k, new_k)
        tc2 = tc2.replace(k, new_k)

    tc3 = simplejson.loads(tc1)
    tc3.update(simplejson.loads(tc2))

    f = tempfile.mkstemp(prefix='tasks_json', suffix=func_name)[1]
    with open(f, 'w') as fout:
        fout.write(simplejson.dumps(tc3))
    return f, renames


def teardown_tasks_json(func_name, tasks_json_tmpfile):
    """Code that runs just after each test"""
    os.remove(tasks_json_tmpfile)


def teardown_queue_backend(func_name):
    qb.get_qbclient().delete(func_name, _recursive=True)
    qb.get_qbclient().delete(makepath(func_name), _recursive=True)


def setup_tasks_json(func_name):
    tasks_json_tmpfile, renames = _create_tasks_json(func_name)
    return ('--tasks_json', tasks_json_tmpfile), dict(
        tasks_json_tmpfile=tasks_json_tmpfile, **dict(renames))


def post_setup_queue_backend(func_name):
    teardown_queue_backend(func_name)  # must happen after initialize(...)
    return dict()


def setup_job_ids(func_name):
    return ((), dict(
        job_id1='20140606_1111_profile-%s' % func_name,
        job_id2='20140606_2222_profile-%s' % func_name,
        job_id3='20140604_1111_profile-%s' % func_name,
        depends_on_job_id1='20140601_testID1-%s' % func_name,
    ))


def with_setup_factory(setup_funcs=(), teardown_funcs=(),
                       post_initialize=()):
    """Create different kinds of `@with_setup` decorators
    to properly initialize and teardown things necessary to run test functions

    `setup_funcs` - a list of functions that do things before a test
    `teardown_funcs` - a list of funcitons that do things after a test
    `post_initialize` - a list of functions to run during setup, but after
        stolos has been initialized

    All functions must receive `func_name` as input.
      - setup and teardown funcs must return (tup, dct) where:
          - the tuple is an optional list of initializer_args to pass
          to Stolos's initialier.
          - the dict is an optional list of keywords that tests can request in
          their function definitions
      - post_initialize funcs return just the dict
    """
    def setup_func(func_name):
        initializer_args = []
        available_kwargs = dict(
            log=util.configure_logging(False, log=logging.getLogger(
                'stolos.tests.%s' % func_name)),
            func_name=func_name,
        )
        for f in setup_funcs:
            tup, dct = f(func_name)
            initializer_args.extend(tup)
            available_kwargs.update(dct)
        _initialize([cb, dt, qb], args=initializer_args)
        for f in post_initialize:
            dct = smart_run(f, (), available_kwargs)
            available_kwargs.update(dct)
        return ((), available_kwargs)

    def teardown_func(*args, **kwargs):
        for f in teardown_funcs:
            smart_run(f, args, kwargs)

    def with_setup_multi(func):
        """Decorator that wraps a test function and provides setup()
        and teardown() functionality.

        The decorated func may define as a parameter any kwargs
        returned by setup(func_name).  The setup() is func.name aware but not
        necessarily module aware, so be careful if using the same test name
        in different test modules.
        """
        return with_setup(setup_func, teardown_func, True)(func)
    return with_setup_multi


default_with_setup = with_setup_factory(
    (setup_job_ids, setup_tasks_json),
    (teardown_tasks_json, teardown_queue_backend),
    (post_setup_queue_backend, )
)


@contextmanager
def inject_into_dag(func_name, inject_dct):
    """Update (add or replace) tasks in dag with new task config.
    Assumes that the config we're using is the JSONMapping
    """
    if not all(k.startswith(makepath(func_name))
               for k in inject_dct):
        raise UserWarning(
            "inject_into_dag can only inject app_names that have the"
            " correct prefix:  %s" % makepath(func_name, '{app_name}'))
    f = _create_tasks_json(func_name, inject=inject_dct)[0]
    _initialize([cb, dt, qb], args=['--tasks_json', f])

    # verify injection worked
    dg = cb.get_tasks_config().to_dict()
    for k, v in inject_dct.items():
        assert dg[k] == v, (
            "test code: inject_into_dag didn't insert the new tasks?")
    yield
    os.remove(f)
