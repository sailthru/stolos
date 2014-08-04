import atexit
import functools
from pyspark import SparkConf, SparkContext
import sys

from . import log


def receive_kwargs_as_dict(func):
    """A decorator that recieves a dict and passes the kwargs to wrapped func.
    It's very useful to use for spark functions:

        @receive_kwargs_as_dict
        def myfunc(a, b):
            return a > 1

        print myfunc({'a': 4, 'b': 6})
        sc.parallelize([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]).filter(myfunc)
    """
    @functools.wraps(func)
    def _partial(kwargs_dct, **kwargs):
        kwargs.update(kwargs_dct)
        return func(**kwargs)
    return _partial


def get_spark_context(conf, osenv={}, files=[], pyFiles=[]):
    """Wrap pyspark.SparkContext.  If SparkContext has already been initialized,
    return the initialized Context

        conf - (dict) a dictionary of key-value configuration
             - or, a pre-configured SparkConf instance
        osenv - (dict) the environment variables to set on executors
        files - (list of str) list of files to make available to executors
        pyFiles - (list of str) list python code to make available to executors

    An example configuration:
        conf = {
            "spark.app.name": "myapp",
            "spark.master": "local[30]",
            "spark.local.dir": "/tmp" }

    """
    if not isinstance(conf, dict):
        assert isinstance(conf, SparkConf)
        conf = dict(conf.getAll())
    _spark_conf = SparkConf()
    for k, v in conf.items():
        _spark_conf.set(k, v)
    conf = _spark_conf

    if osenv:
        conf.setExecutorEnv(pairs=osenv.items())
    try:
        sc = SparkContext(conf=conf)
    except ValueError:
        log.warn("Another Spark Context is already active.  Using that one")
        return SparkContext._active_spark_context
    for method, lst in [(sc.addPyFile, pyFiles), (sc.addFile, files)]:
        for path in lst:
            if not path:
                continue
            method(path)
    atexit.register(sc.stop)
    return sc


def get_module_from_fp(fp):
    """Load a module from given file path
    The module's import path is guaranteed to be in the sys.path
    and therefore serializable by spark"""
    _parts = fp.split('/')
    paths = ('/'.join(_parts[:i]) for i in range(len(_parts) - 1, 0, -1))
    path = '.'  # basecase
    for path in paths:
        if path in sys.path:
            break
    from_package, import_name = (
        fp
        .replace(path + '/', '')
        .replace('/', '.')
        .rsplit('.', 1)
    )
    return __import__(from_package, fromlist=[import_name])
