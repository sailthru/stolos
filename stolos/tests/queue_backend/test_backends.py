import inspect

from stolos import testing_tools as tt
from stolos import get_NS

from . import test_conforms_api
from . import test_return_values


BACKENDS = {
    'zookeeper': ('--qb_zookeeper_hosts', 'localhost:2181', ),
    'redis': ('--qb_redis_host', '127.0.0.1',
              '--qb_redis_port', '6379',
              '--qb_redis_db', '0'),
}


def setup_qb(backend, args):
    def _setup_qb(func_name):
        return (
            ('--queue_backend', backend) + args, dict(
                backend=backend,
                app1=tt.makepath(func_name, 'app1'),
                app2=tt.makepath(func_name, 'app2'),
                app3=tt.makepath(func_name, 'app3'),
                app4=tt.makepath(func_name, 'app4'),
            ))
    return _setup_qb


def with_setup_factory_for_qb(backend, args):
    return tt.with_setup_factory(
        (tt.setup_job_ids, setup_qb(backend, args), ),
        (),
        (tt.post_setup_queue_backend,
         lambda: dict(qbcli=get_NS().queue_backend))
    )


def test_generator(*args, **kwargs):
    for k, v in BACKENDS.items():
        for mod in [test_conforms_api, test_return_values]:
            for fn, f in inspect.getmembers(mod, inspect.isfunction):
                if not fn.startswith('QBtest_'):
                    continue
                test_func = with_setup_factory_for_qb(k, v)(f)
                test_func.description = 'test_backend.%s.%s' % (k, f.__name__)
                yield test_func
