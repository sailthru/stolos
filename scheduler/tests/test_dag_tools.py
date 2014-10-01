from nose import tools as nt

from scheduler import dag_tools
from scheduler import exceptions

from scheduler.testing_tools import configure_logging
log = configure_logging('scheduler.tests.test_dag')


def test_dag_is_valid():
    dag_tools.build_dag()  # validator raises its own asserts


def test_passes_filter():
    nt.assert_false(
        dag_tools.passes_filter(
            'test_scheduler/test_app2', '20130104_876_content')
    )
    nt.assert_true(
        dag_tools.passes_filter(
            'test_scheduler/test_app2', '20130104_876_profile')
    )


def test_job_id_validations():
    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_app2', '20130104_876_')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_app2', '20130104_876a_profile')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(
            'test_scheduler/test_app2', '20130199_876_profile')


def test_missing_job_id_validations_okay():
    """
    does parse_job_id still work if you don't define a validation func?
    """
    items = dag_tools.parse_job_id(
        'test_scheduler/test_custom_job_id', '20130101_876_equal2me')
    nt.assert_equal(items['date'], 20130101)
    nt.assert_equal(items['client_id'], 876)
    nt.assert_equal(items['unvalidated_field'], 'equal2me')


def test_get_children():

    nt.assert_equal(
        list(dag_tools.get_children(
            'test_scheduler/test_pyspark', '20140601_876_profile')),
        [(u'test_scheduler/test_depends_on', u'20140601_2', u'depgrp2')]
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_bash2', '20140601_9899_purchase'))),
        []
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_bash', '20140601_9899_purchase'))),
        [(u'test_scheduler/test_bash2',
          '20140601_9899_purchase', 'default')]
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_app', '20140601_999_purchase'))),
        sorted([
            (u'test_scheduler/test_depends_on', u'20140601_1', u'depgrp1'),
            (u'test_scheduler/test_app2', '20140601_999_purchase', 'default'),
            (u'test_scheduler/test_app4', '20140601_999_purchase', 'default'),
        ])
    )

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_app', '20140601_876_purchase'))),
        sorted([
            (u'test_scheduler/test_depends_on', u'20140601_1', u'depgrp1'),
            (u'test_scheduler/test_app2', '20140601_876_purchase', 'default'),
            (u'test_scheduler/test_app4', '20140601_876_purchase', 'default'),
        ])
    )


def test_get_parents():

    # test case with no parents
    nt.assert_equal(
        list(dag_tools.get_parents(
            'test_scheduler/test_app', '20140101_876_purchase', True)),
        []
    )

    # test the basic inheritance scenario
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_bash2', '20140501_876_profile', True))),
        sorted(
            [(u'test_scheduler/test_bash',
              '20140501_876_profile', 'default')])
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
            (u'test_scheduler/test_pyspark',
             '20140601_1011_profile', u'depgrp2'),
            (u'test_scheduler/test_pyspark',
             '20140601_9020_profile', u'depgrp2'),
            (u'test_scheduler/test_pyspark',
             '20140601_876_profile', u'depgrp2')
        ])
    )

    # test depends_on for one of the dependency groups
    # also tests that get_parents returns a stable ordering
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_1', True))),
        sorted([
            (u'test_scheduler/test_app',
             '20140601_1011_profile', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_1011_purchase', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_9020_profile', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_9020_purchase', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_876_profile', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_876_purchase', u'depgrp1'),
            (u'test_scheduler/test_app',
             '20140601_999_purchase', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_1011_profile', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_1011_purchase', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_9020_profile', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_9020_purchase', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_876_profile', u'depgrp1'),
            (u'test_scheduler/test_app2',
             '20140601_876_purchase', u'depgrp1')
        ])
    )

    # test depends_on when multiple dependency groups map to the same job_id
    # I guess it's okay if they map to the same id?
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True))),
        sorted([
            (u'test_scheduler/test_app',
             '20140601_444_profile', u'depgrp4'),
            (u'test_scheduler/test_app', '20140601_876_profile', u'depgrp3')
        ])
    )

    # test the filter_deps option
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True,
            filter_deps=['depgrp4']))),
        sorted([
            (u'test_scheduler/test_app',
             '20140601_444_profile', u'depgrp4'),
        ])
    )

    with nt.assert_raises(exceptions.DAGMisconfigured):
        list(dag_tools.get_parents(
            'test_scheduler/test_depends_on', '20140601_3', True,
            filter_deps=['depgrp99999']))


def test_fan_out_tasks():
    # test for Many-to-Many relationships between parent and child tasks
    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_fanout', '20140715_8'))),
        [])

    nt.assert_equal(
        sorted(list(dag_tools.get_parents(
            'test_scheduler/test_fanout', '20140715_5', True))),
        sorted([
            (u'test_scheduler/test_app', '20140714_555_profile', u'dep2'),
            (u'test_scheduler/test_app', '20140715_555_profile', u'dep2'),
        ]))

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_app', '20140715_9_profile', True,))),
        [(u'test_scheduler/test_app2', '20140715_9_profile', 'default'),
         (u'test_scheduler/test_app4', '20140715_9_profile', 'default'),
         (u'test_scheduler/test_fanout', '20140715_1', u'dep1'),
         (u'test_scheduler/test_fanout', '20140715_2', u'dep1'),
         (u'test_scheduler/test_fanout', '20140715_3', u'dep1'),
         ])

    nt.assert_equal(
        sorted(list(dag_tools.get_children(
            'test_scheduler/test_app', '20140715_555_profile', True,))),
        [
            (u'test_scheduler/test_app2', '20140715_555_profile', 'default'),
            (u'test_scheduler/test_app4', '20140715_555_profile', 'default'),
            (u'test_scheduler/test_fanout', u'20140714_5', u'dep2'),
            (u'test_scheduler/test_fanout', u'20140714_6', u'dep2'),
            (u'test_scheduler/test_fanout', u'20140715_1', u'dep1'),
            (u'test_scheduler/test_fanout', u'20140715_2', u'dep1'),
            (u'test_scheduler/test_fanout', u'20140715_3', u'dep1'),
            (u'test_scheduler/test_fanout', u'20140715_5', u'dep2'),
            (u'test_scheduler/test_fanout', u'20140715_6', u'dep2'),
        ])


def test_topological_sort():

    nt.assert_equal(
        list(dag_tools.topological_sort(dag_tools.get_parents(
            'test_scheduler/test_topological_sort', '20140601_1', True,))),
        [(u'test_scheduler/test_app', '20140601_101_profile', u'dep1'),
         (u'test_scheduler/test_app', '20140601_102_profile', u'dep1'),
         (u'test_scheduler/test_app2', '20140601_101_profile', u'dep1'),
         (u'test_scheduler/test_app2', '20140601_102_profile', u'dep1'),
         (u'test_scheduler/test_depends_on', u'20140601_1', u'dep1'),
         (u'test_scheduler/test_bash2', '20140601_101_profile', u'dep1'),
         (u'test_scheduler/test_bash2', '20140601_102_profile', u'dep1')]
    )
