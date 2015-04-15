import nose.tools as nt
from os.path import join

from stolos import exceptions


def tests_create_get_children(qbcli, app1):
    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.get_children(app1)

    qbcli.create(app1, 'value')
    with nt.assert_raises(exceptions.NodeExistsError):
        qbcli.create(app1, 'value')
    nt.assert_equal(qbcli.get_children(app1), [])

    qbcli.create(join(app1, 'nested/node'), 'value')
    qbcli.create(join(app1, 'dir1/node'), 'value')
    nt.assert_items_equal(
        qbcli.get_children(app1), ['nested', 'dir1'])
    nt.assert_items_equal(
        qbcli.get_children(join(app1, 'dir1', 'node')), [])


def tests_create_get(qbcli, app1):
    qbcli.create(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.create(join(app1, 'a/b'), 'value2')
    nt.assert_equal(qbcli.get(join(app1, 'a/b')), 'value2')
    nt.assert_equal(qbcli.get(app1), 'value1')

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.get(join(app1, 'noNodeHere'))

    qbcli.create(join(app1, 'c'), 'v1')
    qbcli.create(join(app1, 'C'), 'v2')
    nt.assert_equal(qbcli.get(join(app1, 'c')), 'v1')
    nt.assert_equal(qbcli.get(join(app1, 'C')), 'v2')


def tests_set(qbcli, app1):
    qbcli.create(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.set(app1, 'value1')
    nt.assert_equal(qbcli.get(app1), 'value1')

    qbcli.set(app1, 'value2')
    nt.assert_equal(qbcli.get(app1), 'value2')

    with nt.assert_raises(exceptions.NoNodeError):
        qbcli.set(join(app1, 'noexist'), 'value2')
        # TODO: this
