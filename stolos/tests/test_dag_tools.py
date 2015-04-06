from nose import tools as nt

from stolos import testing_tools as tt
from stolos import dag_tools
from stolos import exceptions

nt.assert_equal.im_class.maxDiff = None


@tt.with_setup
def test_dag_is_valid():
    dag_tools.build_dag(validate=True)  # validator raises its own asserts


@tt.with_setup
def test_passes_filter(app2):
    nt.assert_false(
        dag_tools.passes_filter(app2, '20130104_876_content')
    )
    nt.assert_true(
        dag_tools.passes_filter(app2, '20130104_876_profile')
    )


@tt.with_setup
def test_job_id_validations(app2):
    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, '20130104_876_')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, '20130104_876a_profile')

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, '20130199_876_profile')


@tt.with_setup
def test_missing_job_id_validations_okay(custom_job_id1):
    """
    does parse_job_id still work if you don't define a validation func?
    """
    items = dag_tools.parse_job_id(custom_job_id1, '20130101_876_equal2me')
    nt.assert_equal(items['date'], 20130101)
    nt.assert_equal(items['client_id'], 876)
    nt.assert_equal(items['unvalidated_field'], 'equal2me')


@tt.with_setup
def test_get_children(app1, app2, app4, depends_on1, depends_on2, bash1, bash2):

    nt.assert_items_equal(
        list(dag_tools.get_children(depends_on2, '20140601_876_profile')),
        [(depends_on1, u'20140601_2', u'depgrp2')])

    nt.assert_items_equal(
        list(dag_tools.get_children(bash2, '20140601_9899_purchase')),
        []
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(bash1, '20140601_9899_purchase')),
        [(bash2, '20140601_9899_purchase', 'default')]
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(app1, '20140601_999_purchase')),
        [
            (depends_on1, u'20140601_1', u'depgrp1'),
            (app2, '20140601_999_purchase', 'default'),
            (app4, '20140601_999_purchase', 'default'),
        ]
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(app1, '20140601_876_purchase')),
        [
            (depends_on1, u'20140601_1', u'depgrp1'),
            (app2, '20140601_876_purchase', 'default'),
            (app4, '20140601_876_purchase', 'default'),
        ]
    )


@tt.with_setup
def test_get_parents(app1, app2, depends_on1, depends_on2, bash1, bash2):

    # test case with no parents
    nt.assert_equal(
        list(dag_tools.get_parents(app1, '20140101_876_purchase', True)),
        []
    )

    # test the basic inheritance scenario
    nt.assert_items_equal(
        list(dag_tools.get_parents(bash2, '20140501_876_profile', True)),
        [(bash1, '20140501_876_profile', 'default')]
    )

    # test invalid job_id
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, '20140101_999999', True)),
        []
    )

    # test invalid metadata in job_id
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, '20140601_999', True)),
        []
    )

    # test depends_on for one of the dependency groups
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, '20140601_2', True)),
        [
            (depends_on2, '20140601_1011_profile', u'depgrp2'),
            (depends_on2, '20140601_9020_profile', u'depgrp2'),
            (depends_on2, '20140601_876_profile', u'depgrp2')
        ])

    # test depends_on for one of the dependency groups
    # also tests that get_parents returns a stable ordering
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, '20140601_1', True)),
        [
            (app1, '20140601_1011_profile', u'depgrp1'),
            (app1, '20140601_1011_purchase', u'depgrp1'),
            (app1, '20140601_9020_profile', u'depgrp1'),
            (app1, '20140601_9020_purchase', u'depgrp1'),
            (app1, '20140601_876_profile', u'depgrp1'),
            (app1, '20140601_876_purchase', u'depgrp1'),
            (app1, '20140601_999_purchase', u'depgrp1'),
            (app2, '20140601_1011_profile', u'depgrp1'),
            (app2, '20140601_1011_purchase', u'depgrp1'),
            (app2, '20140601_9020_profile', u'depgrp1'),
            (app2, '20140601_9020_purchase', u'depgrp1'),
            (app2, '20140601_876_profile', u'depgrp1'),
            (app2, '20140601_876_purchase', u'depgrp1')
        ]
    )

    # test depends_on when multiple dependency groups map to the same job_id
    # I guess it's okay if they map to the same id?
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, '20140601_3', True)),
        [(app1, '20140601_444_profile', u'depgrp4'),
         (app1, '20140601_876_profile', u'depgrp3'),
        ]
    )

    # test the filter_deps option
    nt.assert_items_equal(
        list(dag_tools.get_parents(
            depends_on1, '20140601_3', True, filter_deps=['depgrp4'])),
        [(app1, '20140601_444_profile', u'depgrp4')]
    )

    with nt.assert_raises(exceptions.DAGMisconfigured):
        list(dag_tools.get_parents(
            depends_on1, '20140601_3', True, filter_deps=['depgrp99999']))


@tt.with_setup
def test_fan_out_tasks(app1, app2, app4, fanout1 ):
    # test for Many-to-Many relationships between parent and child tasks
    nt.assert_items_equal(
        list(dag_tools.get_parents(
            'test_stolos/test_fan_out_tasks/fanout1', '20140715_8')),
        [])

    nt.assert_items_equal(
        list(dag_tools.get_parents(
            'test_stolos/test_fan_out_tasks/fanout1', '20140715_5', True)),
        [
            (app1, '20140714_555_profile', u'dep2'),
            (app1, '20140715_555_profile', u'dep2'),
        ])

    nt.assert_items_equal(
        list(dag_tools.get_children(
            'test_stolos/test_fan_out_tasks/app1',
            '20140715_9_profile', True,)),
        [(app2, '20140715_9_profile', 'default'),
         (app4, '20140715_9_profile', 'default'),
         (fanout1, '20140715_1', u'dep1'),
         (fanout1, '20140715_2', u'dep1'),
         (fanout1, '20140715_3', u'dep1'),
         ])

    nt.assert_items_equal(
        list(dag_tools.get_children(
            app1, '20140715_555_profile', True,)),
        [
            (app2, '20140715_555_profile', 'default'),
            (app4, '20140715_555_profile', 'default'),
            (fanout1, u'20140714_5', u'dep2'),
            (fanout1, u'20140714_6', u'dep2'),
            (fanout1, u'20140715_1', u'dep1'),
            (fanout1, u'20140715_2', u'dep1'),
            (fanout1, u'20140715_3', u'dep1'),
            (fanout1, u'20140715_5', u'dep2'),
            (fanout1, u'20140715_6', u'dep2'),
        ])


@tt.with_setup
def test_topological_sort(topological_sort1, app1, app2, depends_on1, bash2):
    nt.assert_items_equal(
        list(dag_tools.topological_sort(dag_tools.get_parents(
            topological_sort1, '20140601_1', True,))),
        [
            (app1, '20140601_101_profile', u'dep1'),
            (app1, '20140601_102_profile', u'dep1'),
            (app2, '20140601_101_profile', u'dep1'),
            (app2, '20140601_102_profile', u'dep1'),
            (depends_on1, u'20140601_1', u'dep1'),
            (bash2, '20140601_101_profile', u'dep1'),
            (bash2, '20140601_102_profile', u'dep1')
        ]
    )
