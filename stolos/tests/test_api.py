from nose import tools as nt
from kazoo.exceptions import NoNodeError

from stolos import api
from stolos.testing_tools import configure_logging, with_setup
log = configure_logging('stolos.tests.test_dag')


# TODO all of this
@with_setup
def test_check_state(zk, app1, job_id1):

    nt.assert_false(
        api.check_state(
            app1, job_id1, zk,
            raise_if_not_exists=False,
            pending=False, completed=False, failed=False, skipped=False,
            _get=False))

    with nt.assert_raises(NoNodeError):
        api.check_state(
            app1, job_id1, zk,
            raise_if_not_exists=True,
            pending=False, completed=False, failed=False, skipped=False,
            _get=False)
    # aj


def test_get_qsize():
    # api.get_qsize()
    pass


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
