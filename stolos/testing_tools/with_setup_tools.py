"""
Implements a smarter version of nose.tools.with_setup
"""
import nose.tools as nt
import inspect


def smart_run(func, args, kwargs):
    """Given a function definition, determine which of the args and
    kwargs are relevant and then execute the function"""
    spec = inspect.getargspec(func)
    kwargs2 = {k: v for k, v in kwargs.items() if k in spec.args}
    args2 = args[:len(spec.args) - len(kwargs2)]
    if spec.varargs:
        args2 += args[len(args2):]
    if spec.keywords:
        kwargs2.update(kwargs)
    return func(*args2, **kwargs2)


def with_setup(setup=None, teardown=None, params=False):
    """Decorator to add setup and/or teardown methods to a test function
    Works like nose.tools.with_setup, but adds `params` option.

    `setup` - setup function to run before calling a test function
    `teardown` - teardown function to run after a test function returns
    `params` - (boolean) whether to enable a special mode where:
      - setup will return args and kwargs
      - the test function and teardown will may define any of the values
        returned by setup in their function definitions.


      def setup():
          return dict(abc=123, def=456)

      @_with_setup(setup, params=True)
      def test_something(abc):
          assert abc == 123
    """
    def decorate(func, setup=setup, teardown=teardown):
        args = []
        kwargs = {}
        if params:
            @nt.make_decorator(func)
            def func_wrapped():
                smart_run(func, args, kwargs)
        else:
            func_wrapped = func
        if setup:
            def setup_wrapped():
                if params:
                    rv = setup(func.__name__)
                else:
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
                    smart_run(teardown, args, kwargs)
                func_wrapped.teardown = _t
            else:
                if params:
                    def _sr():
                        smart_run(teardown, args, kwargs)
                    func_wrapped.teardown = _sr
                else:
                    func_wrapped.teardown = teardown
        return func_wrapped
    return decorate
