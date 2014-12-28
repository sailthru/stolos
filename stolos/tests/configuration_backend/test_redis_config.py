import redis
import os
from stolos import testing_tools as tt
import nose.tools as nt
from stolos.configuration_backend.redis_config import (
    RedisMapping, REDIS_PREFIX, set_config)
from stolos.configuration_backend.json_config import (
    JSONMapping, JSONSequence)

from stolos.testing_tools import configure_logging
log = configure_logging(
    'stolos.tests.configuration_backend.test_redis_config')


def make_json():
    dct = {
        'app1': {'key1': 1, 2: 2, 3: [3, 33, 333]},
        'app2': {'key5': 5, 6: 6, 7: [7, 77, 777]},
    }
    return {app_name: {repr(k): repr(v) for k, v in app_config.items()}
            for app_name, app_config in dct.items()}


def _make_key(func_name, app_name):
    return '%s__%s' % (func_name, app_name)


def get_redis_client():
    return redis.StrictRedis(db=int(os.environ.get('STOLOS_REDIS_DB', 0)))


def setup_func(func_name):
    raw = {_make_key(func_name, app_name): data
           for app_name, data in make_json().items()}
    td = RedisMapping()
    cli = get_redis_client()
    for app_name, app_conf in raw.items():
        cli.hmset('%s%s' % (REDIS_PREFIX, app_name), app_conf)
    return [], dict(td=td, raw=raw, cli=cli, **{a: a2 for a, a2 in zip(
        make_json().keys(), raw.keys())})


def teardown_func(raw):
    assert get_redis_client().delete(
        *('%s%s' % (REDIS_PREFIX, app_name)
          for app_name in raw.keys())), ("Oops did not clean up properly?")


def with_setup(func):
    return tt.with_setup(func, setup_func, teardown_func)


@with_setup
def test_get_item(app1, td):
    with nt.assert_raises(KeyError):
        td['appNotExist']
    nt.assert_is_instance(td, RedisMapping)
    nt.assert_is_instance(td[app1], JSONMapping)
    nt.assert_is_instance(td[app1], JSONMapping)
    # are returned values of the correct type?
    nt.assert_items_equal(td[app1].keys(), ['key1', 2, 3])

    nt.assert_is_instance(
        td[app1][3], JSONSequence)
    nt.assert_equal(td[app1]['key1'], 1)
    nt.assert_equal(len(td[app1]), 3)


@with_setup
def test_set_item(td, app1):
    with nt.assert_raises(TypeError):
        td['a'] = {}
    with nt.assert_raises(TypeError):
        td['a'] = 1
    with nt.assert_raises(TypeError):
        td[app1]['b'] = 1


@with_setup
def test_set_config(td, app1, cli):
    dct = {
        '1': '1',
        2: 2,
        1.1: 1.1,
        'a': 1,
        3: ['1', 2],
        4: {'a': 1, '1': '1', 2: 2}}

    # verify set_config does not delete old keys
    nt.assert_in('key1', td[app1])
    nt.assert_equal(1, td[app1]['key1'])
    set_config(app1, dct, cli=cli)
    td = RedisMapping()
    nt.assert_in('key1', td[app1])
    nt.assert_equal(1, td[app1]['key1'])

    # verify set_config adds new keys
    nt.assert_items_equal(td[app1].keys(), ['key1'] + dct.keys())
    nt.assert_equal('1', td[app1]['1'])
    nt.assert_equal(2, td[app1][2])
    nt.assert_equal(1.1, td[app1][1.1])
    nt.assert_equal(1, td[app1]['a'])
    nt.assert_is_instance(
        td[app1][3], JSONSequence)
    nt.assert_is_instance(
        td[app1][4], JSONMapping)
    nt.assert_list_equal(list(td[app1][3]), dct[3])
    nt.assert_dict_equal(dict(td[app1][4]), dct[4])
