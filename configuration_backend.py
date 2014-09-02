import collections
import os
import ujson


class TasksConfigBase(collections.Mapping):

    def __getitem__(self, key):
        # fetch the configuration for an app_name.  This can return a dict,
        # but could also return a new instance of this class or some kind
        # of collections.Mapping  - Remember, a collections.Mapping object is
        # immutable while a normal dict is mutable.
        raise NotImplementedError("You need to write this")

    def __iter__(self):
        # iterate over all known app_names
        raise NotImplementedError("You need to write this")

    def __len__(self):
        # number of apps recognized by the scheduler
        raise NotImplementedError("You need to write this")

    def __repr__(self):
        return "TasksConfig<%s keys:%s>" % (
            self.__class__.__name__, len(self))


class JSONConfig(TasksConfigBase):
    """
    A read-only dictionary loaded with data from a file identified by
    the environment variable, TASKS_JSON
    """
    def __init__(self, data=None):
        if data is None:
            self.cache = ujson.load(open(os.environ['TASKS_JSON']))
        else:
            self.cache = data

    def __getitem__(self, key):
        rv = self.cache[key]
        if isinstance(rv, list):
            return tuple(rv)
        elif isinstance(rv, dict):
            return JSONConfig(rv)
        else:
            return rv

    def __len__(self):
        return len(self.cache)

    def __iter__(self):
        return iter(self.cache)


from scheduler import test_utils as tu
import nose.tools as nt


def setup_func():
    raw = {'a': 1, 'b': [1, 2, 3], 'c': {'aa': 1, 'bb': [1, 2, 3]}}
    td = JSONConfig(raw)
    return [], dict(td=td, raw=raw)


def teardown_func():
    pass


def with_setup(func):
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
