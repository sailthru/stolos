import nose.tools as nt
import inspect

from stolos.queue_backend import qbcli_baseapi


def assert_function_signatures_equal(f1, f2, msg):
    base_spec = inspect.getargspec(f1)
    spec = inspect.getargspec(f2)
    nt.assert_equal(spec, base_spec, msg)


def QBtest_conforms_to_baseapi_interface(qbcli):
    msg = "%s: %%s" % qbcli.__name__
    for varname in dir(qbcli_baseapi):
        if varname.startswith("_"):
            continue
        base_obj = getattr(qbcli_baseapi, varname)

        nt.assert_true(
            hasattr(qbcli, varname), msg % "does not define %s" % varname)

        if inspect.isclass(base_obj):
            kls = getattr(qbcli, varname)
            nt.assert_true(issubclass(kls, base_obj))
            # are class method signatures == in baseapi and the backend?
            _methods = inspect.getmembers(base_obj, inspect.ismethod)
            for method_name, method_func in _methods:
                nt.assert_true(
                    hasattr(kls, method_name),
                    msg % "does not define %s.%s" % (varname, method_name))
                assert_function_signatures_equal(
                    getattr(kls, method_name), method_func,
                    msg % "%s.%s does not have correct method signature" % (
                        varname, method_name))
        elif inspect.isfunction(base_obj):
            # are the function signatures are == in baseapi and the backend?
            f = getattr(qbcli, varname)
            assert_function_signatures_equal(
                f, base_obj, msg %
                "%s does not define the correct function signature" % varname)
            nt.assert_equal(
                base_obj.__name__, f.__name__,
                msg % "%s has wrong function name" % varname)
        else:
            raise NotImplementedError("What is this?")
