import importlib
from nose import tools as nt
from networkx import MultiDiGraph

import stolos
from stolos import api
from stolos import testing_tools as tt
from stolos import queue_backend as qb
from stolos.exceptions import JobAlreadyQueued, InvalidJobId, NoNodeError
from stolos.configuration_backend import TasksConfigBaseMapping
nt.assert_equal.im_class.maxDiff = None


@tt.with_setup
def test_check_state(app1, job_id1, job_id2):

    nt.assert_false(api.check_state(app1, 'doesnotexist', pending=True))
    with nt.assert_raises(NoNodeError):
        api.check_state(app1, 'doesnotexist', raise_if_not_exists=True)

    qb.set_state(app1, job_id1, pending=True)
    # also: create an invalid state (one that stolos does not recognize)
    api.get_qbclient().create(
        qb.get_job_path(app1, job_id2), None)

    with nt.assert_raises(UserWarning):
        api.check_state(app1, job_id1)
    nt.assert_true(
        api.check_state(app1, job_id1, pending=True))
    nt.assert_true(
        api.check_state(app1, job_id1, pending=True, completed=True))
    nt.assert_false(api.check_state(app1, job_id1, completed=True))
    nt.assert_true(api.check_state(app1, job_id1, all=True))
    # the invalid job:
    nt.assert_false(api.check_state(app1, job_id2, all=True))


@tt.with_setup
def test_check_state2(app1, job_id1, job_id2, job_id3):
    """Does check_state support multiple job_ids?"""
    qb.set_state(app1, job_id1, pending=True)
    qb.set_state(app1, job_id2, completed=True)
    qb.set_state(app1, job_id3, failed=True)
    nt.assert_list_equal(
        [True, True],
        api.check_state(
            app1, [job_id1, job_id2], pending=True, completed=True))
    nt.assert_list_equal(
        [True, True],
        api.check_state(
            app1, [job_id1, job_id2], all=True))


@tt.with_setup
def test_get_qsize(app1, job_id1, job_id2):
    nt.assert_equal(api.get_qsize(app1, queued=True, taken=True), 0)
    tt.enqueue(app1, job_id1, )
    tt.enqueue(app1, job_id2, validate_queued=False)
    q = api.get_qbclient().LockingQueue(app1)
    itm = q.get()
    nt.assert_equal(2, api.get_qsize(app1, queued=True, taken=True))
    nt.assert_equal(1, api.get_qsize(app1, queued=False, taken=True))
    nt.assert_equal(1, api.get_qsize(app1, queued=True, taken=False))
    q.consume()
    q.put(itm)
    nt.assert_equal(2, api.get_qsize(app1, queued=True, taken=True))
    nt.assert_equal(0, api.get_qsize(app1, queued=False, taken=True))
    nt.assert_equal(2, api.get_qsize(app1, queued=True, taken=False))


@tt.with_setup
def test_maybe_add_subtask(app1, job_id1, job_id2, job_id3):
    # we don't queue anything if we request queue=False, but we create data for
    # this node if it doesn't exist
    tt.validate_zero_queued_task(app1)
    api.maybe_add_subtask(app1, job_id1, queue=False)
    tt.validate_zero_queued_task(app1)

    # data for this job_id exists, so it can't get queued
    api.maybe_add_subtask(app1, job_id1, priority=4)
    tt.validate_zero_queued_task(app1)

    api.maybe_add_subtask(app1, job_id2, priority=8)
    tt.validate_one_queued_task(app1, job_id2)
    api.maybe_add_subtask(app1, job_id3, priority=5)
    # this should have no effect because it's already queued with priority=5
    api.maybe_add_subtask(app1, job_id3, priority=9)

    job_id = tt.cycle_queue(app1)
    nt.assert_equal(job_id3, job_id)


@tt.with_setup
def test_readd_subtask(app1, job_id1, job_id2):
    # readding the same job twice should result in error and 1 queued job
    tt.validate_zero_queued_task(app1)
    api.readd_subtask(app1, job_id1)
    tt.validate_one_queued_task(app1, job_id1)
    with nt.assert_raises(JobAlreadyQueued):
        api.readd_subtask(app1, job_id1)
    tt.validate_one_queued_task(app1, job_id1)

    # setting task pending but not queueing it.
    api.maybe_add_subtask(app1, job_id2, queue=False)
    tt.validate_one_queued_task(app1, job_id1)
    # then queueing it.
    api.readd_subtask(app1, job_id2)
    tt.validate_n_queued_task(app1, job_id1, job_id2)


@tt.with_setup
def test_get_qbclient(app1):
    qb1 = api.get_qbclient()
    qb2 = api.get_qbclient()
    nt.assert_equal(id(qb1), id(qb1))
    nt.assert_equal(hash(qb1), hash(qb2))

    qb1.create(app1, 'A')
    nt.assert_equal(qb2.get(app1)[0], 'A')


@tt.with_setup
def test_get_tasks_config():
    tc = api.get_tasks_config()
    nt.assert_is_instance(tc, TasksConfigBaseMapping)
    nt.assert_items_equal(
        tc,
        ['test_stolos/test_get_tasks_config/depends_on2',
         'test_stolos/test_get_tasks_config/custom_job_id1',
         'test_stolos/test_get_tasks_config/app2',
         'test_stolos/test_get_tasks_config/topological_sort1',
         'test_stolos/test_get_tasks_config/fanout1',
         'test_stolos/test_get_tasks_config/depends_on1',
         'test_stolos/test_get_tasks_config/bash2',
         'test_stolos/test_get_tasks_config/app3',
         'test_stolos/test_get_tasks_config/app1',
         'test_stolos/test_get_tasks_config/bash1',
         'test_stolos/test_get_tasks_config/app4'])


@tt.with_setup
def test_build_dag():
    dag = api.build_dag()
    nt.assert_is_instance(dag, MultiDiGraph)
    tc = api.get_tasks_config()
    nt.assert_items_equal(tc.keys(), dag.node.keys())


@tt.with_setup
def test_create_job_id(app1, job_id1):
    with nt.assert_raises(KeyError):
        api.create_job_id(app1, a=1)
    with nt.assert_raises(InvalidJobId):
        api.create_job_id(
            app1, date=20141299, client_id=111, collection_name='profile')
    with nt.assert_raises(InvalidJobId):
        api.create_job_id(
            app1, date=20141212, client_id=111, collection_name='Profile')

    nt.assert_equal(
        '20141212_111_profile',
        api.create_job_id(
            app1, date=20141212, client_id=111, collection_name='profile'))


@tt.with_setup
def test_parse_job_id(app1, job_id1):
    nt.assert_dict_equal(
        {'date': 20140606, 'collection_name': 'profile', 'client_id': 1111},
        api.parse_job_id(app1, job_id1))
    nt.assert_equal(
        job_id1,
        api.create_job_id(app1, **api.parse_job_id(app1, job_id1))
    )


@tt.with_setup
def test_get_parents():
    pass  # tested in dag_tools


@tt.with_setup
def test_get_children():
    pass  # tested in dag_tools


@tt.with_setup
def test_topological_sort():
    pass  # tested in dag_tools


def test_not_initialized():
    stolos1 = importlib.import_module('stolos')
    api1 = importlib.import_module('stolos.api')
    nt.assert_true(hasattr(stolos1, 'Uninitialized'))
    with nt.assert_raises(stolos1.Uninitialized):
        stolos1.get_NS()
    api1.initialize([])
    nt.assert_false(hasattr(stolos1, 'Uninitialized'))


@tt.with_setup
def test_initialize():
    with nt.assert_raises(SystemExit):
        api.initialize(['-h'])
    api.initialize(['--configuration_backend', 'json', '--tasks_json', 'a'])
    tj = stolos.get_NS().tasks_json
    api.initialize(['--configuration_backend', 'json', '--tasks_json', 'b'])
    tj2 = stolos.get_NS().tasks_json
    nt.assert_equal(tj, 'a')
    nt.assert_equal(tj2, 'b')


@tt.with_setup
def test_configure_logging(log, func_name):
    nt.assert_equal(log.name, 'stolos.tests.%s' % func_name)


@tt.with_setup
def test_visualize_dag():
    # this should succeeed but do nothing.
    api.visualize_dag(dg=None, plot_nx=False, plot_graphviz=False,
                      write_dot=False, prog='dot')


@tt.with_setup
def test_get_job_id_template(custom_job_id1):
    tc = api.get_tasks_config()
    templ, ptempl = api.get_job_id_template(custom_job_id1)
    nt.assert_equal(templ, tc[custom_job_id1]['job_id'])
    nt.assert_equal(len(ptempl), tc[custom_job_id1]['job_id'].count('{'))
    # sanity check
    nt.assert_equal(
        tc[custom_job_id1]['job_id'].count('{'),
        tc[custom_job_id1]['job_id'].count('}'))
