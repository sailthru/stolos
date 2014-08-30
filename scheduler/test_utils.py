def configure_logging(logname):
    import logging
    from colorlog import ColoredFormatter

    _ignore_log_keys = set(logging.makeLogRecord({}).__dict__)

    def _json_format(record):
        extras = ' '.join(
            "%s=%s" % (k, record.__dict__[k])
            for k in set(record.__dict__).difference(_ignore_log_keys))
        if extras:
            record.msg = "%s    %s" % (record.msg, extras)
        return record

    class ColoredJsonFormatter(ColoredFormatter):
        def format(self, record):
            record = _json_format(record)
            return super(ColoredJsonFormatter, self).format(record)

    log = logging.getLogger('scheduler')
    _formatter = ColoredJsonFormatter(
        "%(log_color)s%(levelname)-8s %(message)s %(reset)s %(cyan)s",
        reset=True)
    _h = logging.StreamHandler()
    _h.setFormatter(_formatter)
    log.handlers = [_h]  # log.addHandler(_h)
    log.setLevel(logging.DEBUG)
    return logging.getLogger(logname)


def smart_run(func, args, kwargs):
    """Given a function definition, determine which of the args and
    kwargs are relevant and then execute the function"""
    fc = func.func_code
    args2 = args[:fc.co_argcount]
    kwargs2 = {k: kwargs[k]
                for k in fc.co_varnames[len(args2): len(args) + 1]}
    print args2, kwargs2
    func(*args2, **kwargs2)


def with_setup_args(setup, teardown=None):
    """Wraps nose.tools.with_setup, and support args/kwargs to test functions

    Slightly modified from:
    https://gist.github.com/garyvdm/392ae20c673c7ee58d76

    Decorator to add setup and/or teardown methods to a test function::

      @with_setup_args(setup, teardown)
      def test_something():
          " ... "

      @with_setup_args(lambda: (['arg1', 'arg2'], {'kw1': 'a', 'kw2': 'b'}

    The setup function should return (args, kwargs) which, if possible,
    will be passed to test function, and teardown function.

    Note that `with_setup_args` is useful *only* for test functions, not for
    test methods or inside of TestCase subclasses.
    """
    def decorate(func):
        args = []
        kwargs = {}

        def test_wrapped():
            smart_run(func, args, kwargs)
        test_wrapped.__name__ = func.__name__

        def setup_wrapped():
            a, k = setup()
            args.extend(a)
            kwargs.update(k)
            if hasattr(func, 'setup'):
                func.setup()
        test_wrapped.setup = setup_wrapped

        if teardown:
            def teardown_wrapped():
                if hasattr(func, 'teardown'):
                    func.teardown()
                smart_run(teardown, args, kwargs)

            test_wrapped.teardown = teardown_wrapped
        else:
            if hasattr(func, 'teardown'):
                test_wrapped.teardown = func.teardown()
        return test_wrapped
    return decorate
