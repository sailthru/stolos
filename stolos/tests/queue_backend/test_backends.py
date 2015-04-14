import inspect
import nose.tools as nt

from stolos import get_NS
from stolos import queue_backend as qb
from stolos import testing_tools as tt
from stolos.queue_backend import qbcli_baseapi


def setup_qb(backend, args):
    return lambda func_name: (
        ('--queue_backend', backend) + args, dict(backend=backend))


# with_setup = lambda backend, args: tt.with_setup_factory(
#     (tt.setup_tasks_json, setup_qb(backend, args), ),
#     (tt.teardown_tasks_json, tt.teardown_queue_backend),
#     (tt.post_setup_queue_backend, )
# )
with_setup = lambda backend, args: tt.with_setup_factory(
    (setup_qb(backend, args), ),
    (),
    ()
)


def conforms_to_baseapi(func_name):
    qbmodule = get_NS().queue_backend
    msg = "%s, %%s" % func_name
    for varname in dir(qbcli_baseapi):
        if varname.startswith("__"):
            continue
        base_obj = getattr(qbcli_baseapi, varname)

        nt.assert_true(
            hasattr(qbmodule, varname), msg % ("does not define %s" % varname))

        if inspect.isclass(base_obj):
            # TODO: are class method signatures == in baseapi and the backend?
            continue
        elif inspect.isfunction(base_obj):
            # are the function signatures are == in baseapi and the backend?
            base_spec = inspect.getargspec(base_obj)
            spec = inspect.getargspec(getattr(qbmodule, varname))
            nt.assert_equal(
                spec, base_spec,
                msg % "does not define the correct args and kwargs")
        else:
            raise NotImplementedError("What is this?")

        # TODO: write test for each baseapi thing function that tests for
        # appropriate outputs?
    raise NotImplementedError()


@with_setup('zookeeper', ('--qb_zookeeper_hosts', 'localhost:2181'))
def test_zookeeper(backend, func_name):
    print(backend)
    conforms_to_baseapi(func_name)


@with_setup('redis', (
    '--qb_redis_host', '127.0.0.1',
    '--qb_redis_port', '6379',
    '--qb_redis_db', '0'))
def test_redis(backend):
    print(backend)
    conforms_to_baseapi(func_name)
