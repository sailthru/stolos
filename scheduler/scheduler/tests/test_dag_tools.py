from nose import tools as nt

from scheduler import dag_tools
from scheduler import exceptions


def test_dag_is_valid():
    dag_tools.build_dag()  # validator raises its own asserts


def test_passes_filter():
    nt.assert_false(
        dag_tools.passes_filter(
            'test_scheduler/test_module2', '20130104_450_content')
    )
    nt.assert_true(
        dag_tools.passes_filter(
            'test_scheduler/test_module2', '20130104_450_profile')
    )


def test_job_id_validations():
    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_module2', '20130104_450_')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_module2', '20130104_450a_profile')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_module2', '20130199_450_profile')


def test_get_children():

    nt.assert_equal(
        list(dag_tools.get_children(
            'test_scheduler/test_minimal', '20140601_450_profile')),
        [(u'test_scheduler/test_depends_on', u'20140601_2', u'depgrp2')]
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_bashworker2', '20140601_9899_purchase'))),
        []
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_bashworker', '20140601_9899_purchase'))),
        [(u'test_scheduler/test_bashworker2',
          '20140601_9899_purchase', 'default')]
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_module', '20140601_999_purchase'))),
        sorted([
            (u'test_scheduler/test_depends_on', u'20140601_1', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_999_purchase', 'default')
        ])
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_module', '20140601_450_purchase'))),
        sorted([
            (u'test_scheduler/test_depends_on', u'20140601_1', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_450_purchase', 'default')
        ])
    )


def test_get_parents():

    # test case with no parents
    nt.assert_equal(
        list(dag_tools.get_parents(
            'test_scheduler/test_module', '20140101_450_purchase', True)),
        []
    )

    # test the basic inheritance scenario
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_bashworker2', '20140501_450_profile', True))),
        sorted(
            [(u'test_scheduler/test_bashworker',
              '20140501_450_profile', 'default')])
    )

    # test invalid job_id
    nt.assert_equal(
        list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140101_999999', True)),
        []
    )

    # test invalid metadata in job_id
    nt.assert_equal(
        list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_999', True)),
        []
    )

    # test depends_on for one of the dependency groups
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_2', True))),
        sorted([
            (u'test_scheduler/test_minimal',
             '20140601_1072_profile', u'depgrp2'),
            (u'test_scheduler/test_minimal',
             '20140601_4023_profile', u'depgrp2'),
            (u'test_scheduler/test_minimal',
             '20140601_450_profile', u'depgrp2')
        ])
    )

    # test depends_on for one of the dependency groups
    # also tests that get_parents returns a stable ordering
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_1', True))),
        sorted([
            (u'test_scheduler/test_module',
             '20140601_1072_profile', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_1072_purchase', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_4023_profile', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_4023_purchase', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_450_profile', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_450_purchase', u'depgrp1'),
            (u'test_scheduler/test_module',
             '20140601_999_purchase', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_1072_profile', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_1072_purchase', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_4023_profile', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_4023_purchase', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_450_profile', u'depgrp1'),
            (u'test_scheduler/test_module2',
             '20140601_450_purchase', u'depgrp1')
        ])
    )

    # test depends_on when multiple dependency groups map to the same job_id
    # I guess it's okay if they map to the same id?
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True))),
        sorted([
            (u'test_scheduler/test_module',
             '20140601_444_profile', u'depgrp4'),
            (u'test_scheduler/test_module', '20140601_450_profile', u'depgrp3')
        ])
    )

    # test the filter_deps option
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True,
            filter_deps=['depgrp4']))),
        sorted([
            (u'test_scheduler/test_module',
             '20140601_444_profile', u'depgrp4'),
        ])
    )

    with nt.assert_raises(exceptions.DAGMisconfigured):
        list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True,
            filter_deps=['depgrp99999']))
