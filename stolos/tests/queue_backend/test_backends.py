from stolos import testing_tools as tt

from .test_conforms_api import conforms_to_baseapi_interface
from .test_return_values import outputs_expected_return_values


TESTS = [
    conforms_to_baseapi_interface,
    outputs_expected_return_values,
]


BACKENDS = {
    'zookeeper': ('--qb_zookeeper_hosts', 'localhost:2181', ),
    'redis': ('--qb_redis_host', '127.0.0.1',
              '--qb_redis_port', '6379',
              '--qb_redis_db', '0'),
}


def setup_qb(backend, args):
    return lambda func_name: (
        ('--queue_backend', backend) + args, dict(
            backend=backend,
            app1=tt.makepath(func_name, 'app1'),
            app2=tt.makepath(func_name, 'app2'),
            app3=tt.makepath(func_name, 'app3'),
            app4=tt.makepath(func_name, 'app4'),
        ))


with_setup = lambda backend, args: tt.with_setup_factory(
    (tt.setup_job_ids, setup_qb(backend, args), ),
    (),
    (tt.post_setup_queue_backend, )
)


def test_backend(*args, **kwargs):
    for k, v in BACKENDS:
        for f in TESTS:
            yield with_setup(k, v)(f)
