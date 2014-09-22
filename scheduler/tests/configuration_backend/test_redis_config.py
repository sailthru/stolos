import redis
import os
from scheduler import testing_tools as tt
import nose.tools as nt
from scheduler.configuration_backend.redis_config import (
    RedisMapping, REDIS_PREFIX)
from scheduler.configuration_backend.json_config import (
    JSONMapping, JSONSequence)

from scheduler.dag_tools import get_tasks_config

from scheduler.testing_tools import configure_logging
log = configure_logging(
    'scheduler.tests.configuration_backend.test_redis_config')


def make_json():
    return {
        'app1': {'key1': 1, 2: 2, 3: [3, 33, 333]},
        'app2': {'key5': 5, 6: 6, 7: [7, 77, 777]},
    }


def _make_key(func_name, app_name):
    return '%s__%s' % (func_name, app_name)


def get_client():
    return redis.StrictRedis(db=int(os.environ.get('SCHEDULER_REDIS_DB', 0)))


def setup_func(func_name):
    raw = {_make_key(func_name, app_name): data
           for app_name, data in make_json().items()}
    td = RedisMapping()
    cli = get_client()
    for app_name, app_conf in raw.items():
        cli.hmset('%s%s' % (REDIS_PREFIX, app_name), app_conf)
    return [], dict(td=td, raw=raw, **{a: a2 for a, a2 in zip(
        make_json().keys(), raw.keys())})


def teardown_func(raw):
    assert get_client().delete(*('%s%s' % (REDIS_PREFIX, app_name)
                                 for app_name in raw.keys())), (
                                     "Oops did not clean up properly?")


def with_setup(func):
    return tt.with_setup(
        lambda: setup_func(func.func_name), teardown_func, True)(func)


@with_setup
def test_get_item(app1, td):
    with nt.assert_raises(KeyError):
        td['appNotExist']
    nt.assert_is_instance(td, RedisMapping)
    nt.assert_is_instance(td[app1], JSONMapping)
    raise NotImplementedError()
    # nt.assert_is_instance(td[app1][3], JSONSequence)  # TODO requires hiredis
    # nt.assert_equal(td[app1]['key1'], 1)  # TODO hiredis


@with_setup
def test_set_item(td, app1):
    with nt.assert_raises(TypeError):
        td['a'] = {}
    with nt.assert_raises(TypeError):
        td['a'] = 1
    with nt.assert_raises(TypeError):
        td[app1]['b'] = 1
