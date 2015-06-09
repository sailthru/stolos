import functools
import simplejson
import importlib

from stolos.plugins import at, log_and_raise, api, log

from . import pyspark_context


def main(ns):
    """
    A generic plugin that uses Spark to:
        read data,
        transform data with given code,
        and maybe write transformed data

    Assume code is written in Python.  For Scala or R code, use another option.
    """
    job_id = ns.job_id
    module = get_pymodule(ns.app_name)

    pjob_id = api.parse_job_id(ns.app_name, job_id)
    log_details = dict(
        module_name=module.__name__,
        app_name=ns.app_name, job_id=job_id)

    conf, osenv, files, pyFiles = pyspark_context.get_spark_conf(ns.app_name)
    conf['spark.app.name'] = "%s__%s" % (conf['spark.app.name'], job_id)
    conf.update(ns.spark_conf)
    osenv.update(ns.spark_env)
    sc = pyspark_context.get_spark_context(
        conf=conf, osenv=osenv, files=files, pyFiles=pyFiles)
    apply_data_transform(
        ns=ns, sc=sc, log_details=log_details, pjob_id=pjob_id, module=module)
    sc.stop()


def get_pymodule(app_name):
    dg = api.get_tasks_config()
    module_name = dg[app_name]['pymodule']
    return importlib.import_module(module_name)


def pre_process_data(ns, tf, log_details):
    """
    For convenience, perform operations on the input stream before passing
    along to other data processing code
    """
    if ns.sample:
        log.info('sampling a percentage of input data without replacement',
                 extra=dict(sampling_pct=ns.sample, **log_details))
        tf = tf.sample(False, ns.sample, 0)
    if ns.mapjson:
        log.info('converting all elements in data stream to json')
        tf = tf.map(simplejson.loads)
    return tf


def apply_data_transform(ns, sc, log_details, pjob_id, module):
    """Pass control to the module.main method.  If module.main specifies a
    `textFile` parameter, pass the textFile instance.  Otherwise, just map
    module.main on the RDD
    """
    func_args = (module.main.func_code
                 .co_varnames[:module.main.func_code.co_argcount])
    if 'sc' in func_args:
        log.info(
            'passing spark context to a module.main function',
            extra=log_details)
        try:
            module.main(sc=sc, ns=ns, **pjob_id)
        except Exception as err:
            log_and_raise(
                "Job failed with error: %s" % err, log_details)
    else:
        read_fp = format_fp(ns.read_fp, ns, pjob_id)
        log_details = dict(read_fp=read_fp, **log_details)
        tf = sc.textFile(read_fp, ns.minPartitions)
        tf = pre_process_data(ns=ns, tf=tf, log_details=log_details)
        if 'textFile' in func_args:
            log.info(
                'passing textFile instance to a module.main function',
                extra=log_details)
            try:
                module.main(textFile=tf, ns=ns, **pjob_id)
            except Exception as err:
                log_and_raise(
                    "Job failed with error: %s" % err, log_details)

        else:
            write_fp = format_fp(ns.write_fp, ns, pjob_id)
            log.info(
                'mapping a module.main function to all elements in a textFile'
                ' and writing output',
                extra=dict(write_fp=write_fp, **log_details))
            try:
                (
                    tf
                    .map(functools.partial(module.main, ns=ns, **pjob_id))
                    .saveAsTextFile(write_fp)
                )
            except Exception as err:
                log_and_raise(err, log_details)


def format_fp(fp, ns, pjob_id):
    kwargs = dict(**ns.__dict__)
    kwargs.update(pjob_id)
    return fp.format(**kwargs)


def _validate_sample_size(str_i):
    """Helper for --sample argument option in argparse"""
    i = float(str_i)
    assert 0 <= i <= 1, "given sample size must be a number between [0, 1]"
    return i


_build_arg_parser = at.build_arg_parser([at.group(
    'Spark Job Options: How should given module.main process data?',
    at.app_name,
    at.group(
        "Preprocess data",
        at.add_argument(
            '--mapjson', action='store_true', help=(
                'convert each element in the textFile to json before doing'
                ' anything with it.')),
        at.add_argument(
            '--sample', type=_validate_sample_size,
            help="Sample n percent of the data without replacement"),
    ),
    at.add_argument('--read_fp'),
    at.add_argument('--write_fp'),
    at.add_argument(
        '--spark_conf', nargs='*', type=lambda x: x.split('='), default=[],
        help=("A list of key=value pairs that override"
              " the task's default settings. ie:"
              " spark.master=local[4] spark.ui.port=4046")),
    at.add_argument(
        '--spark_env', nargs='+', type=lambda x: x.split('='), default=[],
        help=("A list of key=value pairs to add to the spark executor's"
              " environment.  ie:"
              " --spark_env MYVAR=foo APP_CONFIG=bar")),
    at.add_argument(
        '--minPartitions', type=int, help=("2nd argument passed to textFile")),
)])


def build_arg_parser():
    """
    Get the app's parser, have it inherit from this parser
    (which may inherit from other parsers)
    Return the a new parser that inherits from app's parser,
    from this parser, and implicitly from this parser's parents (if it has any)

    I'm doing this so that python spark apps can have a simple, standard design
    """
    parents = [_build_arg_parser()]

    _ns, _ = parents[0].parse_known_args()
    _app = get_pymodule(_ns.app_name)
    if hasattr(_app, 'build_arg_parser'):
        parents.append(
            _app.build_arg_parser()
        )
    return at.build_arg_parser(parents=parents)
