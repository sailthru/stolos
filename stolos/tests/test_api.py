from nose import tools as nt
from kazoo.exceptions import NoNodeError

from stolos import api
from stolos import testing_tools as tt
from stolos import zookeeper_tools as zkt
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


# TODO all of this
def test_maybe_add_subtask():
    pass


def test_readd_subtask():
    pass


def test_get_zkclient():
    """docstring for test_get_zkclient"""
    pass


def test_get_tasks_config():
    pass


def test_build_dag():
    pass


def test_visualize_dag():
    pass


def test_create_job_id():
    pass


def test_parse_job_id():
    pass


def test_get_parents():
    pass


def test_get_children():
    pass


def test_delete():
    pass


def test_requeue():
    pass


def test_requeue_failed():
    pass


def test_get_job_ids_by_status():
    pass


def test_get_failed():
    pass
