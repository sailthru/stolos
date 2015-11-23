from stolos import testing_tools as tt
import nose.tools as nt
from stolos.configuration_backend.redis_config import (
    RedisMapping, set_config)
from stolos.configuration_backend.json_config import (
    JSONMapping, JSONSequence)

REDIS_PREFIX = 'test_stolos/%s/%s'


def make_json():
    dct = {
        'app1': {'key1': 1, 2: 2, 3: [3, 33, 333]},
        'app2': {'key5': 5, 6: 6, 7: [7, 77, 777]},
    }
    return {app_name: {repr(k): repr(v) for k, v in app_config.items()}
            for app_name, app_config in dct.items()}


def setup_redis(func_name):
    return (
        '--configuration_backend', 'redis',
        '--redis_key_prefix', '',
    ), dict()


def post_initialize(func_name):
    raw = make_json()
    app_names = {
        app_name: REDIS_PREFIX % (func_name, app_name) for app_name in raw}
    td = RedisMapping()
    for app_name, app_conf in raw.items():
        td.cli.hmset(app_names[app_name], app_conf)
    return dict(
        cli=td.cli, raw=raw, td=td, **app_names)


def teardown_redis(raw, cli, func_name):
    assert cli.delete(
        *(REDIS_PREFIX % (func_name, app_name)
          for app_name in raw)), \
        "Oops did not clean up redis properly"


with_setup = tt.with_setup_factory(
    [setup_redis], [teardown_redis], [post_initialize])


@with_setup
def test_get_item(app1, td):
    with nt.assert_raises(KeyError):
        td['appNotExist']
    nt.assert_is_instance(td, RedisMapping)
    nt.assert_is_instance(td[app1], JSONMapping)
    nt.assert_is_instance(td[app1], JSONMapping)
    # are returned values of the correct type?
    nt.assert_equal(len(list(td[app1].keys())), 3)
    nt.assert_true(all(x in [3, 2, 'key1'] for x in td[app1].keys()))
    nt.assert_is_instance(
        td[app1][3], JSONSequence)
    nt.assert_equal(td[app1]['key1'], 1)
    nt.assert_equal(len(td[app1]), 3)


@with_setup
def test_set_item(app1, td):
    with nt.assert_raises(TypeError):
        td['a'] = {}
    with nt.assert_raises(TypeError):
        td['a'] = 1
    with nt.assert_raises(TypeError):
        td[app1]['b'] = 1


@with_setup
def test_set_config(app1, cli, td):
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
    td = RedisMapping()  # reset's RedisMapping's cache, from prev line
    set_config(app1, dct, cli=cli)
    nt.assert_in('key1', td[app1])
    nt.assert_equal(1, td[app1]['key1'])

    # verify set_config adds new keys
    nt.assert_equal(
        len(list(td[app1].keys())), len(list(dct.keys()) + ['key1']))
    nt.assert_true(all(
        x in (list(dct.keys()) + ['key1']) for x in td[app1].keys()))
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
