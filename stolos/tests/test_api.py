from nose import tools as nt
from kazoo.exceptions import NoNodeError
import os
from networkx import MultiDiGraph

from stolos import api
from stolos import testing_tools as tt
from stolos import zookeeper_tools as zkt
from stolos.exceptions import JobAlreadyQueued
from stolos.configuration_backend import TasksConfigBaseMapping
log = tt.configure_logging('stolos.tests.test_dag')


@tt.with_setup
def test_check_state(zk, app1, job_id1):

    nt.assert_false(api.check_state(app1, job_id1, zk))
    with nt.assert_raises(NoNodeError):
        api.check_state(app1, job_id1, zk, raise_if_not_exists=True)

    zkt.set_state(app1, job_id1, zk=zk, pending=True)

    with nt.assert_raises(UserWarning):
        api.check_state(app1, job_id1, zk)
    with nt.assert_raises(UserWarning):
        api.check_state(app1, job_id1, zk, pending=True, completed=True)
    nt.assert_true(api.check_state(app1, job_id1, zk, pending=True))
    nt.assert_false(api.check_state(app1, job_id1, zk, completed=True))


@tt.with_setup
def test_get_qsize(zk, app1, job_id1, job_id2):
    with nt.assert_raises(NoNodeError):
        api.get_qsize(app1, zk, queued=True, taken=True)
    tt.enqueue(app1, job_id1, zk=zk)
    tt.enqueue(app1, job_id2, zk=zk, validate_queued=False)
    q = zk.LockingQueue(app1)
    itm = q.get(.1)
    nt.assert_equal(2, api.get_qsize(app1, zk, queued=True, taken=True))
    nt.assert_equal(1, api.get_qsize(app1, zk, queued=False, taken=True))
    nt.assert_equal(1, api.get_qsize(app1, zk, queued=True, taken=False))
    q.consume()
    q.put(itm)
    nt.assert_equal(2, api.get_qsize(app1, zk, queued=True, taken=True))
    nt.assert_equal(0, api.get_qsize(app1, zk, queued=False, taken=True))
    nt.assert_equal(2, api.get_qsize(app1, zk, queued=True, taken=False))


@tt.with_setup
def test_maybe_add_subtask(zk, app1, job_id1, job_id2, job_id3):
    # we don't queue anything if we request queue=False, but we create data for
    # this node if it doesn't exist
    tt.validate_zero_queued_task(zk, app1)
    api.maybe_add_subtask(app1, job_id1, zk=zk, queue=False)
    tt.validate_zero_queued_task(zk, app1)

    # data for this job_id exists, so it can't get queued
    api.maybe_add_subtask(app1, job_id1, zk=zk, priority=4)
    tt.validate_zero_queued_task(zk, app1)

    api.maybe_add_subtask(app1, job_id2, zk=zk, priority=8)
    tt.validate_one_queued_task(zk, app1, job_id2)
    api.maybe_add_subtask(app1, job_id3, zk=zk, priority=5)
    # this should have no effect because it's already queued with priority=5
    api.maybe_add_subtask(app1, job_id3, zk=zk, priority=9)

    job_id = tt.cycle_queue(zk, app1)
    nt.assert_equal(job_id3, job_id)


@tt.with_setup
def test_readd_subtask(app1, job_id1, job_id2, zk):
    # readding the same job twice should result in error and 1 queued job
    tt.validate_zero_queued_task(zk, app1)
    api.readd_subtask(app1, job_id1, zk)
    tt.validate_one_queued_task(zk, app1, job_id1)
    with nt.assert_raises(JobAlreadyQueued):
        api.readd_subtask(app1, job_id1, zk)
    tt.validate_one_queued_task(zk, app1, job_id1)

    # setting task pending but not queueing it.
    api.maybe_add_subtask(app1, job_id2, zk=zk, queue=False)
    tt.validate_one_queued_task(zk, app1, job_id1)
    # then queueing it.
    api.readd_subtask(app1, job_id2, zk)
    tt.validate_two_queued_task(zk, app1, job_id1, job_id2)


@tt.with_setup
def test_get_zkclient():
    zk1 = api.get_zkclient()
    zk2 = api.get_zkclient()
    nt.assert_equal(id(zk1), id(zk1))
    nt.assert_equal(hash(zk1), hash(zk2))
    h, p = os.environ['ZOOKEEPER_HOSTS'].split(':')
    nt.assert_equal(zk1.hosts, [(h, int(p))])


@tt.with_setup
def test_get_tasks_config():
    tc = api.get_tasks_config()
    nt.assert_is_instance(tc, TasksConfigBaseMapping)
    nt.assert_items_equal(
        tc,
        [u'test_stolos/test_depends_on2__test_get_tasks_config',
         u'test_stolos/test_custom_job_id__test_get_tasks_config',
         u'test_stolos/test_app2__test_get_tasks_config',
         u'test_stolos/test_topological_sort__test_get_tasks_config',
         u'test_stolos/test_fanout__test_get_tasks_config',
         u'test_stolos/test_depends_on__test_get_tasks_config',
         u'test_stolos/test_bash2__test_get_tasks_config',
         u'test_stolos/test_app3__test_get_tasks_config',
         u'test_stolos/test_app__test_get_tasks_config',
         u'test_stolos/test_bash__test_get_tasks_config',
         u'test_stolos/test_app4__test_get_tasks_config'])


@tt.with_setup
def test_build_dag():
    dag = api.build_dag()
    nt.assert_is_instance(dag, MultiDiGraph)
    tc = api.get_tasks_config()
    nt.assert_items_equal(tc.keys(), dag.node.keys())


@tt.with_setup
def test_visualize_dag():
    pass


@tt.with_setup
def test_create_job_id():
    pass


@tt.with_setup
def test_parse_job_id():
    pass


@tt.with_setup
def test_get_parents():
    pass


@tt.with_setup
def test_get_children():
    pass


@tt.with_setup
def test_delete():
    pass


@tt.with_setup
def test_requeue():
    pass


@tt.with_setup
def test_requeue_failed():
    pass


@tt.with_setup
def test_get_job_ids_by_status():
    pass


@tt.with_setup
def test_get_failed():
    pass
