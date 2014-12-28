from stolos import testing_tools as tt
import nose.tools as nt
from stolos.configuration_backend.json_config import (
    JSONMapping, JSONSequence)


def setup_func(func_name):
    raw = {'a': 1, 'b': [1, 2, 3], 'c': {'aa': 1, 'bb': [1, 2, 3]}}
    td = JSONMapping(raw)
    return [], dict(td=td, raw=raw)


def teardown_func():
    pass


def with_setup(func):
    return tt.with_setup(func, setup_func, teardown_func)


@with_setup
def test_get_item(td, raw):
    nt.assert_is_instance(td, JSONMapping)
    nt.assert_equal(td['a'], 1)
    nt.assert_equal(td['b'], JSONSequence(raw['b']))
    nt.assert_equal(td['b'][0], 1)
    nt.assert_equal(td['c'], JSONMapping(raw['c']))
    nt.assert_equal(td['c']['bb'][-1], 3)


@with_setup
def test_set_item(td, raw):
    with nt.assert_raises(TypeError):
        td['d'] = 1
