from nose import tools as nt
from kazoo.exceptions import NoNodeError
import os
from networkx import MultiDiGraph

from stolos import api
from stolos import testing_tools as tt
from stolos import zookeeper_tools as zkt
from stolos.exceptions import JobAlreadyQueued, InvalidJobId
from stolos.configuration_backend import TasksConfigBaseMapping
log = tt.configure_logging('stolos.tests.test_dag')


@tt.with_setup
def test_check_state(zk, app1, job_id1, job_id2):

    nt.assert_false(api.check_state(app1, job_id1, zk))
    with nt.assert_raises(NoNodeError):
        api.check_state(app1, job_id1, zk, raise_if_not_exists=True)

    zkt.set_state(app1, job_id1, zk=zk, pending=True)
    # also: create an invalid state (one that stolos does not recognize)
    zk.create(zkt._get_zookeeper_path(app1, job_id2), None, makepath=True)

    with nt.assert_raises(UserWarning):
        api.check_state(app1, job_id1, zk)
    nt.assert_true(
        api.check_state(app1, job_id1, zk, pending=True))
    nt.assert_true(
        api.check_state(app1, job_id1, zk, pending=True, completed=True))
    nt.assert_false(api.check_state(app1, job_id1, zk, completed=True))
    nt.assert_true(api.check_state(app1, job_id1, zk, all=True))
    # the invalid job:
    nt.assert_false(api.check_state(app1, job_id2, zk, all=True))


@tt.with_setup
def test_check_state2(zk, app1, job_id1, job_id2, job_id3):
    """Does check_state support multiple job_ids?"""
    zkt.set_state(app1, job_id1, zk=zk, pending=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, failed=True)
    nt.assert_list_equal(
        [True, True],
        api.check_state(
            app1, [job_id1, job_id2], zk=zk, pending=True, completed=True))
    nt.assert_list_equal(
        [True, True],
        api.check_state(
            app1, [job_id1, job_id2], zk=zk, all=True))


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
    tt.validate_n_queued_task(zk, app1, job_id1, job_id2)


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
def test_delete(app1, job_id1, job_id2, zk):
    api.maybe_add_subtask(app1, job_id1, zk)
    api.maybe_add_subtask(app1, job_id2, zk)
    tt.validate_n_queued_task(zk, app1, job_id1, job_id2)

    api.delete(app1, job_id2, zk, confirm=False)
    tt.validate_one_queued_task(zk, app1, job_id1)

    api.maybe_add_subtask(app1, job_id2, zk)
    tt.validate_n_queued_task(zk, app1, job_id1, job_id2)

    api.delete(app1, [job_id1, job_id2], zk, confirm=False)
    tt.validate_zero_queued_task(zk, app1)


@tt.with_setup
def test_requeue(app1, job_id1, job_id2, job_id3, zk):
    zkt.set_state(app1, job_id1, zk=zk, failed=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, skipped=True)

    tt.validate_zero_queued_task(zk, app1)
    api.requeue(app1, zk, confirm=False, pending=True)
    tt.validate_zero_queued_task(zk, app1)

    api.requeue(app1, zk, confirm=False, completed=True)
    tt.validate_one_queued_task(zk, app1, job_id2)

    api.requeue(app1, zk, confirm=False, skipped=True, failed=True)
    tt.validate_n_queued_task(zk, app1, job_id1, job_id2, job_id3)


@tt.with_setup
def test_requeue2(app1, job_id1, job_id2, job_id3, zk):
    zkt.set_state(app1, job_id1, zk=zk, failed=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, skipped=True)
    api.requeue(app1, zk, confirm=False, all=True, regexp=r'.*_1211_.*')
    tt.validate_zero_queued_task(zk, app1)

    api.requeue(app1, zk, confirm=False, all=True, regexp=r'.*_1111_.*')
    tt.validate_n_queued_task(zk, app1, job_id1, job_id3)


@tt.with_setup
def test_requeue3(app1, job_id1, job_id2, job_id3, zk):
    zkt.set_state(app1, job_id1, zk=zk, failed=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, skipped=True)
    api.requeue(app1, zk, confirm=False, failed=True, regexp=r'.*_1111_.*')
    tt.validate_n_queued_task(zk, app1, job_id1)


@tt.with_setup
def test_requeue4(app1, job_id1, job_id2, job_id3, zk):
    zkt.set_state(app1, job_id1, zk=zk, failed=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, skipped=True)
    api.requeue(app1, zk, confirm=False, all=True)
    tt.validate_n_queued_task(zk, app1, job_id1, job_id2, job_id3)


@tt.with_setup
def test_get_job_ids_by_status(app1, job_id1, job_id2, job_id3, zk):
    zkt.set_state(app1, job_id1, zk=zk, failed=True)
    zkt.set_state(app1, job_id2, zk=zk, completed=True)
    zkt.set_state(app1, job_id3, zk=zk, skipped=True)
    nt.assert_list_equal(
        [],
        api.get_job_ids_by_status(app1, zk, regexp=r'.*_1211_.*'))
    nt.assert_list_equal(
        [u'20140606_1111_profile', u'20140604_1111_profile'],
        api.get_job_ids_by_status(app1, zk, regexp=r'.*_1111_.*'))
    nt.assert_list_equal(
        [u'20140606_2222_profile'],
        api.get_job_ids_by_status(app1, zk, regexp=r'.*_2222_.*'))
    nt.assert_list_equal(
        [u'20140606_1111_profile', u'20140604_1111_profile',
         u'20140606_2222_profile'],
        api.get_job_ids_by_status(app1, zk, all=True))
    nt.assert_list_equal(
        api.get_job_ids_by_status(app1, zk, all=True),
        api.get_job_ids_by_status(app1, zk))
    nt.assert_list_equal(
        api.get_job_ids_by_status(app1, zk, all=True),
        api.get_job_ids_by_status(
            app1, zk, failed=True, completed=True, skipped=True))
    nt.assert_list_equal(
        [u'20140606_1111_profile', u'20140606_2222_profile'],
        api.get_job_ids_by_status(app1, zk, completed=True, failed=True))
    nt.assert_list_equal(
        [u'20140606_1111_profile'],
        api.get_job_ids_by_status(
            app1, zk, completed=True, failed=True, regexp=r'.*_1111_.*'))
