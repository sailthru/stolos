import nose.tools as nt

from stolos import get_NS
from stolos import queue_backend as qb
from stolos import testing_tools as tt
from stolos.queue_backend import qbcli_baseapi


def setup_qb(backend, args):
    return lambda func_name: (
        ('--queue_backend', backend) + args, dict(backend=backend))


with_setup = lambda backend, args: tt.with_setup_factory(
    (tt.setup_tasks_json, setup_qb(backend, args)),
    (tt.teardown_tasks_json, tt.teardown_queue_backend),
    (tt.post_setup_queue_backend, )
)


def conforms_to_baseapi():
    qbmodule = get_NS().queue_backend
    for obj in dir(qbcli_baseapi):
        if obj.startswith("__"):
            continue
        nt.assert_true(hasattr(qbmodule, obj), obj)
        base_obj = getattr(qbcli_baseapi, obj)
        obj = getattr(qbmodule, obj)
        # TODO: test that the functions have same keys/values
        # TODO: write test for each baseapi thing

    raise NotImplementedError()


@with_setup('zookeeper', ('--qb_zookeeper_hosts', 'localhost:2181'))
def test_zookeeper(backend):
    print(backend)
    conforms_to_baseapi()


@with_setup('redis', (
    '--qb_redis_host', '127.0.0.1',
    '--qb_redis_port', '6379',
    '--qb_redis_db', '0'))
def test_redis(backend):
    print(backend)
    conforms_to_baseapi()
