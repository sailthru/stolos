import nose.tools as nt
from os.path import join

from stolos import exceptions


def QBtest_create_get_children(qbcli, app1):
    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.get_children(app1)

    qbcli.create(app1, 'value')
    with nt.assert_raises(exceptions.NodeExistsError):
        qbcli.create(app1, 'value')
    nt.assert_equal(qbcli.get_children(app1), [])

    qbcli.create(join(app1, 'nested/node'), 'value')
    qbcli.create(join(app1, 'dir1/node'), '')
    nt.assert_items_equal(
        qbcli.get_children(app1), ['nested', 'dir1'])
    nt.assert_items_equal(
        qbcli.get_children(join(app1, 'nested', 'node')), [])


def QBtest_create_get(qbcli, app1, app2):
    qbcli.create(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.create(app2, '')
    nt.assert_equal(qbcli.get(app2), '')

    qbcli.create(join(app1, 'a/b'), 'value2')
    nt.assert_equal(qbcli.get(join(app1, 'a/b')), 'value2')
    nt.assert_equal(qbcli.get(app1), 'value1')

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.get(join(app1, 'noNodeHere'))

    qbcli.create(join(app1, 'c'), 'v1')
    qbcli.create(join(app1, 'C'), 'v2')
    nt.assert_equal(qbcli.get(join(app1, 'c')), 'v1')
    nt.assert_equal(qbcli.get(join(app1, 'C')), 'v2')


def QBtest_set(qbcli, app1):
    qbcli.create(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.set(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.set(app1, 'value2')
    nt.assert_equal(qbcli.get(app1), 'value2')

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.set(join(app1, 'noexist'), 'value2')


def QBtest_exists(qbcli, app1, app2, app3):
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app2, 'value1')
    nt.assert_true(qbcli.exists(app2))

    qbcli.create(app3, '')
    nt.assert_true(qbcli.exists(app3))


def QBtest_count_children(qbcli, app1, app2):
    qbcli.create(join(app1, 'a'), '')
    nt.assert_equal(qbcli.count_children(app1), 1)

    qbcli.create(join(app1, 'b'), '')
    nt.assert_equal(qbcli.count_children(app1), 2)

    qbcli.create(join(app1, 'c'), '')
    nt.assert_equal(qbcli.count_children(app1), 3)

    qbcli.set(join(app1, 'c'), 'a')
    nt.assert_equal(qbcli.count_children(app1), 3)

    qbcli.set(join(app1, ''), 'a')
    nt.assert_equal(qbcli.count_children(app1), 3)

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.count_children(app2)


def QBtest_delete(qbcli, app1, app2):
    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.delete(app1)

    qbcli.create(app1, 'a')
    nt.assert_true(qbcli.exists(app1))

    qbcli.delete(app1)
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app1, '')
    nt.assert_true(qbcli.exists(app1))

    qbcli.delete(app1)
    nt.assert_false(qbcli.exists(app1))

    # recursive option
    qbcli.create(join(app2, 'a'), '')
    with nt.assert_raises(exceptions.NodeExistsError):
        qbcli.delete(app2, recursive=False)
    nt.assert_true(qbcli.exists(app2))

    qbcli.delete(app2, recursive=True)
    nt.assert_false(qbcli.exists(app2))


def QBtest_Lock(qbcli, app1, app2):
    lock = qbcli.Lock(app1)
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app1, '')
    lock2 = qbcli.Lock(app1)
    nt.assert_not_equal(lock, lock2)

    # acquire lock 1st time
    nt.assert_true(lock.acquire(timeout=1))
    nt.assert_true(lock.acquire(timeout=1))
    nt.assert_true(lock.acquire(blocking=True))
    nt.assert_true(lock.acquire(blocking=False))

    # should not hang
    nt.assert_false(lock2.acquire(blocking=False))
    # should timeout
    nt.assert_false(lock2.acquire(blocking=True, timeout=1))

    with nt.assert_raises(UserWarning):
        lock2.release()
    lock.release()
    nt.assert_true(lock2.acquire(blocking=False, timeout=1))
    lock2.release()


def QBtest_LockingQueue(qbcli, app1):
    raise NotImplementedError("no tests here yet")
