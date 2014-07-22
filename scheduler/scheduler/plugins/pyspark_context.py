from ds_commons import util
from ds_commons.log import log

import atexit
import functools
from pyspark import SparkConf, SparkContext
import sys


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


def get_s3_fp(ns, read, write, **fp_kwargs):
    """
    Given a key, bucket and credentials, return one or two AWS S3 uris

    `ns` an argparse.Namespace containing at least the following:
        ns.aws_s3_creds_fp - (str) some filepath
        ... see below for its other required fields
    `read` - (bool) If True, also assume input ns contains:
        ns.s3_read_key - (str) name of a key in S3
        ns.s3_read_bucket - (str) name of a bucket in S3
        ns.s3_read_scheme - (str) s3:// or s3n://
    `write` - (bool) If True, also assume input ns contains:
        ns.s3_write_key - (str) name of a key in S3
        ns.s3_write_bucket - (str) name of a bucket in S3
        ns.s3_write_scheme - (str) s3:// or s3n://
    `fp_kwargs` - Any extra kwargs passed in can string format the returned fp
        ie: the_fp_to_return.format(**fp_kwargs)

    Return (read_fp, write_fp)
    """
    rfp, wfp = '', ''
    if read:
        if getattr(ns, 'read_fp', None):
            rfp = ns.read_fp.format(**fp_kwargs)
        elif getattr(ns, 's3_read_key', None):
            rfp = _get_s3_fp(
                ns, ns.s3_read_key, ns.s3_read_bucket, ns.s3_read_scheme,
                **fp_kwargs)
        log.info("created a read fp", extra=dict(read_fp=rfp))
    if write:
        if getattr(ns, 'write_fp', None):
            wfp = ns.write_fp.format(**fp_kwargs)
        elif getattr(ns, 's3_write_key', None):
            wfp = _get_s3_fp(
                ns, ns.s3_write_key, ns.s3_write_bucket, ns.s3_write_scheme,
                **fp_kwargs)
        log.info("created a write fp", extra=dict(write_fp=wfp))
    return (rfp, wfp)


def get_module_from_fp(fp):
    """Load a module from given file path
    The module's import path is guaranteed to be in the sys.path
    and therefore serializable by spark"""
    _parts = fp.split('/')
    paths = ('/'.join(_parts[:i]) for i in range(len(_parts) - 1, 0, -1))
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


def _get_s3_fp(ns, s3_key, s3_bucket, s3_scheme, **fp_kwargs):
    kwargs = {}
    kwargs.update(s3_key=s3_key, s3_bucket=s3_bucket, s3_scheme=s3_scheme)
    kwargs.update(ns.__dict__)
    kwargs.update(fp_kwargs)
    # get s3 credentials if 1) creds file is known and 2) key/secret is listed
    # in file
    try:
        creds = util.load_ini_config(ns.aws_s3_creds_fp, s3_bucket).copy()
        creds_str = '{aws_access_key_id}:{aws_secret_access_key}@'.format(
            **creds)
    except (IOError, KeyError) as err:
        log.debug(
            "not using aws s3 credentials because: %s" % err,
            extra=dict(s3_key=s3_key, s3_bucket=s3_bucket,
                       aws_s3_creds_fp=ns.aws_s3_creds_fp))
        creds_str = ''
    kwargs.update(creds_str=creds_str)
    kwargs['s3_key'] = s3_key.format(**kwargs)
    fp = (
        "{s3_scheme}{creds_str}{s3_bucket}/{s3_key}").format(**kwargs)
    return fp
