import inspect

from stolos import testing_tools as tt
from stolos import get_NS

BACKENDS = {
    'zookeeper': (
        '--qb_zookeeper_hosts', 'localhost:2181',
        '--qb_zookeeper_timeout', '1',
    ),
    'majorityredis': (
        '--qb_redis_hosts', '127.0.0.1:6379',
        '--qb_redis_db', '0',
        '--qb_redis_n_servers', '1',
        '--qb_redis_socket_timeout', '2',
    ),
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
                item1="%s-%s" % (func_name, 'a'),
                item2="%s-%s" % (func_name, 'b'),
                item3="%s-%s" % (func_name, 'c'),
                item4="%s-%s" % (func_name, 'd'),
                item5="%s-%s" % (func_name, 'e'),
                item6="%s-%s" % (func_name, 'f'),
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
        for mod_name in ['test_conforms_api', 'test_return_values']:
            mod = __import__(mod_name, globals(), locals(), ['object'], -1)
            for fn, f in inspect.getmembers(mod, inspect.isfunction):
                if not fn.startswith('QBtest_'):
                    continue
                # prefix the func name with the identifier for this backend
                fn = "%s__%s" % (k, fn)
                # wrap the test func with setup and teardown for nose
                f.__name__ = fn
                test_func = with_setup_factory_for_qb(k, v)(f)
                test_func.description = 'test_backend.%s.%s' % (k, fn)
                yield test_func
