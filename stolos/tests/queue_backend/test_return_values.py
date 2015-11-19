import nose.tools as nt
from os.path import join

from stolos import exceptions
from . import with_setup


@with_setup
def test_create_exists1(qbcli, app1, item1):
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app1, item1)
    with nt.assert_raises(exceptions.NodeExistsError):
        qbcli.create(app1, item1)
    nt.assert_true(qbcli.exists(app1))


@with_setup
def test_create_exists2(qbcli, app1, item1):
    nt.assert_false(qbcli.exists(join(app1, 'nested/node')))
    nt.assert_false(qbcli.exists(join(app1, 'dir1/node')))
    qbcli.create(join(app1, 'nested/node'), item1)
    qbcli.create(join(app1, 'dir1/node'), '')
    nt.assert_true(qbcli.exists(join(app1, 'nested/node')))
    nt.assert_true(qbcli.exists(join(app1, 'dir1/node')))


@with_setup
def test_create_get1(qbcli, app1, app2, item1, item2):
    qbcli.create(app1, item1)
    nt.assert_equal(qbcli.get(app1), item1)

    qbcli.create(app2, '')
    nt.assert_equal(qbcli.get(app2), '')

    qbcli.create(join(app1, 'a/b'), item2)
    nt.assert_equal(qbcli.get(join(app1, 'a/b')), item2)
    nt.assert_equal(qbcli.get(app1), item1)

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.get(join(app1, 'noNodeHere'))


@with_setup
def test_create_get_case_sensitive(qbcli, app1, item1, item2):
    qbcli.create(join(app1, 'c'), item1)
    qbcli.create(join(app1, 'C'), item2)
    nt.assert_equal(qbcli.get(join(app1, 'c')), item1)
    nt.assert_equal(qbcli.get(join(app1, 'C')), item2)


@with_setup
def test_set1(qbcli, app1, item1, item2):
    qbcli.create(app1, item1)
    nt.assert_equal(qbcli.get(app1), item1)

    qbcli.set(app1, item1)
    nt.assert_equal(qbcli.get(app1), item1)

    qbcli.set(app1, item2)
    nt.assert_equal(qbcli.get(app1), item2)

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.set(join(app1, 'noexist'), item2)


@with_setup
def test_exists1(qbcli, app1, app2, app3, item1):
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app2, item1)
    nt.assert_true(qbcli.exists(app2))

    qbcli.create(app3, '')
    nt.assert_true(qbcli.exists(app3))


@with_setup
def test_delete1(qbcli, app1, app2):
    # del non-existent node
    nt.assert_false(qbcli.exists(app1))
    nt.assert_false(qbcli.delete(app1))
    nt.assert_false(qbcli.exists(app1))

    # del existing node
    qbcli.create(app1, '')
    nt.assert_true(qbcli.exists(app1))
    nt.assert_true(qbcli.delete(app1))
    nt.assert_false(qbcli.exists(app1))
    nt.assert_false(qbcli.delete(app1))


@with_setup
def test_delete_recursive(qbcli, app1, app2, item1):
    qbcli.create(join(app2, item1), '')
    nt.assert_true(qbcli.exists(join(app2, item1)))

    qbcli.delete(app2, _recursive=True)
    nt.assert_false(qbcli.exists(app2))
    nt.assert_false(qbcli.exists(join(app2, item1)))


@with_setup
def test_Lock1(qbcli, app1):
    lock = qbcli.Lock(app1)
    nt.assert_false(qbcli.exists(app1))

    lock2 = qbcli.Lock(app1)
    nt.assert_not_equal(lock, lock2)

    # acquire lock 1st time
    nt.assert_true(lock.acquire(blocking=True, timeout=1))

    # should not hang
    nt.assert_false(lock2.acquire())
    # should timeout
    # TODO: with nt.assert_raises(exceptions.Timeout):
    nt.assert_false(lock2.acquire(blocking=True, timeout=1))

    with nt.assert_raises(UserWarning):
        lock2.release()
    lock.release()
    nt.assert_true(lock2.acquire())
    lock2.release()


@with_setup
def test_Lock_is_locked(qbcli, app1, app2):
    l = qbcli.Lock(app1)
    nt.assert_false(l.is_locked())
    nt.assert_true(l.acquire())
    nt.assert_true(l.is_locked())

    l2 = qbcli.Lock(app1)
    nt.assert_true(l2.is_locked())
    nt.assert_false(l2.acquire())


@with_setup
def test_Lock_paths(qbcli, app1, app2):
    # respects different paths as different locks
    lock1 = qbcli.Lock(app1)
    lock2 = qbcli.Lock(app2)
    nt.assert_true(lock1.acquire())
    nt.assert_true(lock2.acquire())


@with_setup
def test_LockingQueue_put_paths(qbcli, app1, app2, item1, item2):
    # respects different paths as different queues
    queue = qbcli.LockingQueue(app1)
    queue2 = qbcli.LockingQueue(app2)

    queue.put(item1)
    queue2.put(item2)
    nt.assert_equal(queue2.get(), item2)
    nt.assert_equal(queue.get(), item1)


@with_setup
def test_LockingQueue_put_get(qbcli, app1, item1, item2):
    nt.assert_false(qbcli.exists(app1))
    # instantiating LockingQueue does not create any objects in backend
    queue = qbcli.LockingQueue(app1)
    nt.assert_equal(queue.size(), 0)

    # fail if consuming before you've gotten anything
    with nt.assert_raises(UserWarning):
        queue.consume()

    # get nothing from an empty queue (and don't fail!)
    nt.assert_is_none(queue.get())

    # put item in queue
    nt.assert_equal(queue.size(), 0)
    queue.put(item1)
    queue.put(item2)
    queue.put(item1)
    nt.assert_equal(queue.size(), 3)

    nt.assert_equal(queue.get(0), item1)
    nt.assert_equal(queue.get(), item1)

    # Multiple LockingQueue instances can address the same path
    queue2 = qbcli.LockingQueue(app1)
    nt.assert_equal(queue2.size(), 3)
    nt.assert_equal(queue2.get(), item2)
    nt.assert_equal(queue.get(), item1)  # ensure not somehow mutable or linked


@with_setup
def test_LockingQueue_put_priority(
        qbcli, app1, item1, item2, item3, item4, item5, item6):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    queue2 = qbcli.LockingQueue(app1)

    queue.put(item2, 50)
    queue.put(item3, 60)
    queue.put(item5, 70)
    queue.put(item4, 40)
    queue2.put(item1, 20)
    queue2.put(item6, 80)

    # get in prioritized order, from different LockingQueue objects
    queue3 = qbcli.LockingQueue(app1)
    nt.assert_equal(queue.get(), item1)
    nt.assert_equal(queue2.get(), item4)
    queue.consume()
    nt.assert_equal(queue.get(), item2)
    queue.consume()
    nt.assert_equal(queue.get(), item3)
    queue.consume()
    queue2.consume()
    nt.assert_equal(queue3.get(), item5)
    queue3.consume()
    nt.assert_equal(queue2.get(), item6)


@with_setup
def test_LockingQueue_put_insertion_order(
        qbcli, app1, item1, item2, item3):
    # insertion order matters, too
    queue = qbcli.LockingQueue(app1)
    queue2 = qbcli.LockingQueue(app1)
    queue2.put(item2)
    queue2.put(item3)
    queue2.put(item1)
    nt.assert_equal(queue.get(), item2)
    queue.consume()
    nt.assert_equal(queue.get(), item3)
    queue.consume()
    nt.assert_equal(queue.get(), item1)
    queue.consume()


@with_setup
def test_LockingQueue_consume_size(qbcli, app1, item1, item2):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    with nt.assert_raises(UserWarning):
        queue.consume()

    queue.put(item1)
    queue.put(item2)
    nt.assert_equal(queue.get(), item1)

    nt.assert_equal(queue.size(), 2)
    nt.assert_is_none(queue.consume())
    nt.assert_equal(queue.size(queued=False), 0)
    nt.assert_equal(queue.size(taken=False), 1)

    nt.assert_equal(queue.get(), item2)
    nt.assert_equal(queue.size(queued=False), 1)
    nt.assert_equal(queue.size(taken=False), 0)
    nt.assert_equal(queue.size(), 1)
    nt.assert_is_none(queue.consume())
    nt.assert_equal(queue.size(), 0)


@with_setup
def test_LockingQueue_size(qbcli, app1, item1, item2):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    with nt.assert_raises(AttributeError):
        queue.size(taken=False, queued=False)

    nt.assert_equal(queue.size(), 0)
    nt.assert_equal(queue.size(taken=True, queued=False), 0)
    nt.assert_equal(queue.size(taken=False, queued=True), 0)
    queue.put(item1)
    nt.assert_equal(queue.size(), 1)
    queue.put(item2)
    queue.put(item1)
    nt.assert_equal(queue.size(), 3)
    nt.assert_equal(queue.size(taken=True, queued=True), 3)

    # test various parameters of queue.size
    nt.assert_equal(queue.get(), item1)
    nt.assert_equal(queue.size(taken=True, queued=False), 1)
    nt.assert_equal(queue.size(queued=False), 1)
    nt.assert_equal(queue.size(taken=False, queued=True), 2)
    nt.assert_equal(queue.size(taken=False), 2)


@with_setup
def test_LockingQueue_is_queued(qbcli, app1, item1):
    q = qbcli.LockingQueue(app1)
    nt.assert_false(q.is_queued(item1))
    q.put(item1)
    nt.assert_true(q.is_queued(item1))
