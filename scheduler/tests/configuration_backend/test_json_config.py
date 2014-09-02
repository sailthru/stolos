from scheduler import test_utils as tu
import nose.tools as nt
from scheduler.configuration_backends import JSONConfig


def with_setup(func):

    def setup_func():
        raw = {'a': 1, 'b': [1, 2, 3], 'c': {'aa': 1, 'bb': [1, 2, 3]}}
        td = JSONConfig(raw)
        return [], dict(td=td, raw=raw)

    def teardown_func():
        pass

    return tu.with_setup(setup_func, teardown_func, True)(func)


@with_setup
def test_get_item(td, raw):
    nt.assert_is_instance(td, JSONConfig)
    nt.assert_equal(td['a'], 1)
    nt.assert_equal(td['b'], (1, 2, 3))
    nt.assert_equal(td['c'], JSONConfig(raw['c']))


@with_setup
def test_set_item(td, raw):
    with nt.assert_raises(TypeError):
        td['d'] = 1
