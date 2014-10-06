"""
Useful utilities for testing
"""


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


def _smart_run(func, args, kwargs):
    """Given a function definition, determine which of the args and
    kwargs are relevant and then execute the function"""
    fc = func.func_code
    kwargs2 = {
        k: kwargs[k]
        for k in kwargs if k in fc.co_varnames[:fc.co_nlocals]}
    args2 = args[:fc.co_nlocals - len(kwargs2)]
    func(*args2, **kwargs2)


def with_setup(setup=None, teardown=None, params=False):
    """Decorator to add setup and/or teardown methods to a test function::

      @with_setup(setup, teardown)
      def test_something():
          " ... "

    Note that `with_setup` is useful *only* for test functions, not for test
    methods or inside of TestCase subclasses.
    """
    import nose.tools as nt

    def decorate(func, setup=setup, teardown=teardown):
        args = []
        kwargs = {}
        if params:
            @nt.make_decorator(func)
            def func_wrapped():
                _smart_run(func, args, kwargs)
        else:
            func_wrapped = func
        if setup:
            def setup_wrapped():
                rv = setup()
                if rv is not None:
                    args.extend(rv[0])
                    kwargs.update(rv[1])

            if hasattr(func, 'setup'):
                _old_s = func.setup

                def _s():
                    setup_wrapped()
                    _old_s()
                func_wrapped.setup = _s
            else:
                if params:
                    func_wrapped.setup = setup_wrapped
                else:
                    func_wrapped.setup = setup

        if teardown:
            if hasattr(func, 'teardown'):
                _old_t = func.teardown

                def _t():
                    _old_t()
                    _smart_run(teardown, args, kwargs)
                func_wrapped.teardown = _t
            else:
                if params:
                    func_wrapped.teardown = lambda: _smart_run(
                        teardown, args, kwargs)
                else:
                    func_wrapped.teardown = teardown
        return func_wrapped
    return decorate
