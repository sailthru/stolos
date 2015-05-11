import nose.tools as nt
from os.path import join

from stolos import exceptions


def QBtest_create_exists(qbcli, app1):
    nt.assert_false(qbcli.exists(app1))

    qbcli.create(app1, 'value')
    with nt.assert_raises(exceptions.NodeExistsError):
        qbcli.create(app1, 'value')
    nt.assert_true(qbcli.exists(app1))

    nt.assert_false(qbcli.exists(join(app1, 'nested/node')))
    nt.assert_false(qbcli.exists(join(app1, 'dir1/node')))
    qbcli.create(join(app1, 'nested/node'), 'value')
    qbcli.create(join(app1, 'dir1/node'), '')
    nt.assert_true(qbcli.exists(join(app1, 'nested/node')))
    nt.assert_true(qbcli.exists(join(app1, 'dir1/node')))


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


def QBtest_delete(qbcli, app1, app2):
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


def QBtest_delete_recursive(qbcli, app1, app2):
    qbcli.create(join(app2, 'a'), '')
    nt.assert_true(qbcli.exists(join(app2, 'a')))

    qbcli.delete(app2, _recursive=True)
    nt.assert_false(qbcli.exists(app2))
    nt.assert_false(qbcli.exists(join(app2, 'a')))


def QBtest_Lock(qbcli, app1):
    lock = qbcli.Lock(app1)
    nt.assert_false(qbcli.exists(app1))

    lock2 = qbcli.Lock(app1)
    nt.assert_not_equal(lock, lock2)

    # acquire lock 1st time
    nt.assert_true(lock.acquire(timeout=1))

    # should not hang
    nt.assert_false(lock2.acquire(blocking=False))
    # should timeout
    # TODO: with nt.assert_raises(exceptions.Timeout):
    nt.assert_false(lock2.acquire(blocking=True, timeout=1))

    with nt.assert_raises(UserWarning):
        lock2.release()
    lock.release()
    nt.assert_true(lock2.acquire(blocking=False))
    lock2.release()


def QBtest_Lock_is_locked(qbcli, app1, app2):
    raise NotImplementedError("TODO")


def QBtest_Lock_paths(qbcli, app1, app2):
    # respects different paths as different locks
    lock1 = qbcli.Lock(app1)
    lock2 = qbcli.Lock(app2)
    nt.assert_true(lock1.acquire(blocking=False))
    nt.assert_true(lock2.acquire(blocking=False))


def QBtest_LockingQueue_put_paths(qbcli, app1, app2):
    # respects different paths as different queues
    queue = qbcli.LockingQueue(app1)
    queue2 = qbcli.LockingQueue(app2)

    queue.put('a')
    queue2.put('c')
    nt.assert_equal(queue2.get(), 'c')
    nt.assert_equal(queue.get(), 'a')


def QBtest_LockingQueue_put_get(qbcli, app1):
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
    queue.put('a')
    queue.put('b')
    queue.put('a')
    nt.assert_equal(queue.size(), 3)

    nt.assert_equal(queue.get(0), 'a')
    nt.assert_equal(queue.get(), 'a')

    # Multiple LockingQueue instances can address the same path
    queue2 = qbcli.LockingQueue(app1)
    nt.assert_equal(queue2.size(), 3)
    nt.assert_equal(queue2.get(), 'b')
    nt.assert_equal(queue.get(), 'a')  # ensure not somehow mutable or linked


def QBtest_LockingQueue_put_priority(qbcli, app1):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    queue2 = qbcli.LockingQueue(app1)

    queue.put('b', 50)
    queue.put('c', 60)
    queue.put('e', 70)
    queue.put('d', 40)
    queue2.put('a', 20)
    queue2.put('f', 80)

    # get in prioritized order, from different LockingQueue objects
    queue3 = qbcli.LockingQueue(app1)
    nt.assert_equal(queue.get(), 'a')
    nt.assert_equal(queue2.get(), 'd')
    queue.consume()
    nt.assert_equal(queue.get(), 'b')
    queue.consume()
    nt.assert_equal(queue.get(), 'c')
    queue.consume()
    queue2.consume()
    nt.assert_equal(queue3.get(), 'e')
    queue3.consume()
    nt.assert_equal(queue2.get(), 'f')

    # insertion order matters, too
    queue2.put('B')
    queue2.put('C')
    queue2.put('A')
    nt.assert_equal(queue.get(), 'B')
    queue.consume()
    nt.assert_equal(queue.get(), 'C')
    queue.consume()
    nt.assert_equal(queue.get(), 'A')
    queue.consume()


def QBtest_LockingQueue_consume_size(qbcli, app1):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    with nt.assert_raises(UserWarning):
        queue.consume()

    queue.put('a')
    queue.put('b')
    nt.assert_equal(queue.get(), 'a')

    nt.assert_equal(queue.size(), 2)
    nt.assert_is_none(queue.consume())
    nt.assert_equal(queue.size(queued=False), 0)
    nt.assert_equal(queue.size(taken=False), 1)

    nt.assert_equal(queue.get(), 'b')
    nt.assert_equal(queue.size(queued=False), 1)
    nt.assert_equal(queue.size(taken=False), 0)
    nt.assert_equal(queue.size(), 1)
    nt.assert_is_none(queue.consume())
    nt.assert_equal(queue.size(), 0)


def QBtest_LockingQueue_size(qbcli, app1):
    nt.assert_false(qbcli.exists(app1))
    queue = qbcli.LockingQueue(app1)
    with nt.assert_raises(AttributeError):
        queue.size(taken=False, queued=False)

    nt.assert_equal(queue.size(), 0)
    nt.assert_equal(queue.size(taken=True, queued=False), 0)
    nt.assert_equal(queue.size(taken=False, queued=True), 0)
    queue.put('a')
    nt.assert_equal(queue.size(), 1)
    queue.put('b')
    queue.put('a')
    nt.assert_equal(queue.size(), 3)
    nt.assert_equal(queue.size(taken=True, queued=True), 3)

    # test various parameters of queue.size
    queue.get()
    nt.assert_equal(queue.size(taken=True, queued=False), 1)
    nt.assert_equal(queue.size(queued=False), 1)
    nt.assert_equal(queue.size(taken=False, queued=True), 2)
    nt.assert_equal(queue.size(taken=False), 2)


def QBtest_LockingQueue_is_queued(qbcli, app1):
    raise NotImplementedError("TODO")
