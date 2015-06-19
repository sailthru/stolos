from nose import tools as nt

from stolos import testing_tools as tt
from stolos import dag_tools
from stolos import exceptions

# nt.assert_equal.im_class.maxDiff = None


@tt.with_setup
def test_dag_is_valid():
    dag_tools.build_dag(validate=True)  # validator raises its own asserts


@tt.with_setup
def test_passes_filter(app2, job_id1):
    nt.assert_false(
        dag_tools.passes_filter(
            app2, job_id1.replace('profile', 'content'))
    )
    nt.assert_true(
        dag_tools.passes_filter(app2, job_id1)
    )


@tt.with_setup
def test_job_id_validations(app2, job_id1):
    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, job_id1.replace('profile', ''))

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, job_id1.replace('1111', '11aa'))

    with nt.assert_raises(exceptions.InvalidJobId):
        dag_tools.parse_job_id(app2, job_id1.replace('20140606', '20149606'))


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
def test_get_autofill_values(autofill1, autofill2):
    nt.assert_dict_equal(
        dag_tools.get_autofill_values(autofill1),
        {"client_id": range(10, 20, 2)})
    nt.assert_dict_equal(
        dag_tools.get_autofill_values(autofill2),
        {"date": [20150101, 20150102, 20150103, 20150104]}
    )


@tt.with_setup
def test_autofill_all(func_name, autofill1, autofill2, autofill3,
                      autofill_getparents):
    # autofill1 out of bounds get_children
    nt.assert_items_equal(list(dag_tools.get_children(autofill1, '9')), [])
    nt.assert_items_equal(list(dag_tools.get_children(autofill1, '11')), [])
    nt.assert_items_equal(list(dag_tools.get_children(autofill1, '20')), [])
    # autofill1 in bounds get_children
    nt.assert_items_equal(
        list(dag_tools.get_children(autofill1, '10')),
        [
            (autofill3, '20150101', 'default'),
            (autofill3, '20150102', 'default'),
            (autofill3, '20150103', 'default'),
            (autofill3, '20150104', 'default'),
            (autofill_getparents, '20150101_10_10', 'default'),
        ])

    # autofill2 out of bounds get_children
    nt.assert_items_equal(
        list(dag_tools.get_children(autofill2, '20150128')), [])
    # autofill2 in bounds get_children
    nt.assert_items_equal(
        list(dag_tools.get_children(autofill2, '20150101')),
        [
            (autofill3, '20150101', 'default'),
            (autofill_getparents, '20150101_10_10', 'default')
        ])

    # autofill3 get parents out and in bounds
    nt.assert_items_equal(
        list(dag_tools.get_children(autofill3, '20150101')), [])
    nt.assert_items_equal(
        list(dag_tools.get_parents(autofill3, '20150128')), [])
    nt.assert_items_equal(
        list(dag_tools.get_parents(autofill3, '20150101')),
        [
            (autofill1, '10'),
            (autofill1, '12'),
            (autofill1, '14'),
            (autofill1, '16'),
            (autofill1, '18'),
            (autofill2, '20150101'),
        ])


@tt.with_setup
def test_autofill_get_parents(autofill1, autofill2, autofill_getparents):
    # test when child job_id is a superset of parents and depends_on only
    # defines 2+ app_names (where each app has different job_id templates)
    # omg this is specific!
    nt.assert_items_equal(
        list(dag_tools.get_parents(autofill_getparents, '20150101_10_10')),
        [(autofill1, '10'), (autofill2, '20150101')]
    )


@tt.with_setup
def test_get_children(func_name, app1, app2, app4, depends_on1,
                      depends_on2, bash1, bash2):

    nt.assert_items_equal(
        list(dag_tools.get_children(
            depends_on2, '20140601_876_profile-%s' % func_name)),
        [(depends_on1, u'20140601_testID2-%s' % func_name, u'depgrp2')])

    nt.assert_items_equal(
        list(dag_tools.get_children(bash2, '20140601_9899_purchase')),
        []
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(bash1, '20140601_9899_purchase')),
        [(bash2, '20140601_9899_purchase', 'default')]
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(
            app1, '20140601_999_purchase-%s' % func_name)),
        [
            (depends_on1, u'20140601_testID1-%s' % func_name, u'depgrp1'),
            (app2, '20140601_999_purchase-%s' % func_name, 'default'),
            (app4, '20140601_999_purchase-%s' % func_name, 'default'),
        ]
    )

    nt.assert_items_equal(
        list(dag_tools.get_children(
            app1, '20140601_876_purchase-%s' % func_name)),
        [
            (depends_on1, u'20140601_testID1-%s' % func_name, u'depgrp1'),
            (app2, '20140601_876_purchase-%s' % func_name, 'default'),
            (app4, '20140601_876_purchase-%s' % func_name, 'default'),
        ]
    )


@tt.with_setup
def test_get_children_with_complicated_job_ids(
        func_name, valid1, valid2, valid3, valid3b, valid4):
    nt.assert_items_equal(
        list(dag_tools.get_children(valid1, "20151015_2")),
        [
            (valid4, '20151015_101', 'default'),
            (valid3b, '20151015_101',
             'another_way_to_state_same_thing'),
            (valid3b, '20151015_102',
             'another_way_to_state_same_thing'),
            (valid3, '20151015_101', 'default'),
            (valid3, '20151015_102', 'default')
        ]
    )
    nt.assert_items_equal(
        list(dag_tools.get_children(valid1, "20151015_3-%s" % func_name)),
        []
    )
    nt.assert_items_equal(
        list(dag_tools.get_children(valid2, "20151015_101")),
        [
            (valid4, '20151015_101', 'default'),
            (valid3b, '20151015_101',
             'another_way_to_state_same_thing'),
            (valid3, '20151015_101', 'default')
        ]
    )
    nt.assert_items_equal(
        list(dag_tools.get_children(valid2, "20151015_103")),
        []
    )


@tt.with_setup
def test_get_parents_with_complicated_job_ids(
        func_name, valid1, valid2, valid3, valid3b, valid4):
    nt.assert_items_equal(
        list(dag_tools.get_parents(valid3, '20151015_100')),
        []
    )
    nt.assert_items_equal(
        list(dag_tools.get_parents(valid3, '20151015_101')),
        [
            (valid1, '20151015_1'),
            (valid1, '20151015_2'),
            (valid2, '20151015_101')
        ]
    )
    # valid3b should be same as valid3
    nt.assert_items_equal(
        list(dag_tools.get_parents(valid3b, '20151015_100')),
        list(dag_tools.get_parents(valid3, '20151015_100')),
    )

    nt.assert_items_equal(
        list(dag_tools.get_parents(valid4, '20151015_100')),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_parents(valid4, '20151015_102')),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_parents(valid4, '20151015_101')),
        [
            (valid1, '20151015_1'),
            (valid1, '20151015_2'),
            (valid2, '20151015_101')
        ]
    )


@tt.with_setup
def test_get_parents(app1, app2, depends_on1, depends_on2, bash1, bash2,
                     depends_on_job_id1, func_name):

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
        list(dag_tools.get_parents(
            depends_on1, '20140601_testID2-%s' % func_name, True)),
        [
            (depends_on2, '20140601_1011_profile-%s' % func_name, u'depgrp2'),
            (depends_on2, '20140601_9020_profile-%s' % func_name, u'depgrp2'),
            (depends_on2, '20140601_876_profile-%s' % func_name, u'depgrp2')
        ])

    # test depends_on for one of the dependency groups
    # also tests that get_parents returns a stable ordering
    nt.assert_items_equal(
        list(dag_tools.get_parents(depends_on1, depends_on_job_id1, True)),
        [
            (app1, '20140601_1011_profile-%s' % func_name, u'depgrp1'),
            (app1, '20140601_1011_purchase-%s' % func_name, u'depgrp1'),
            (app1, '20140601_9020_profile-%s' % func_name, u'depgrp1'),
            (app1, '20140601_9020_purchase-%s' % func_name, u'depgrp1'),
            (app1, '20140601_876_profile-%s' % func_name, u'depgrp1'),
            (app1, '20140601_876_purchase-%s' % func_name, u'depgrp1'),
            (app1, '20140601_999_purchase-%s' % func_name, u'depgrp1'),
            (app2, '20140601_1011_profile-%s' % func_name, u'depgrp1'),
            (app2, '20140601_1011_purchase-%s' % func_name, u'depgrp1'),
            (app2, '20140601_9020_profile-%s' % func_name, u'depgrp1'),
            (app2, '20140601_9020_purchase-%s' % func_name, u'depgrp1'),
            (app2, '20140601_876_profile-%s' % func_name, u'depgrp1'),
            (app2, '20140601_876_purchase-%s' % func_name, u'depgrp1')
        ]
    )

    # test depends_on when multiple dependency groups map to the same job_id
    # I guess it's okay if they map to the same id?
    nt.assert_items_equal(
        list(dag_tools.get_parents(
            depends_on1, '20140601_testID3-%s' % func_name, True)),
        [(app1, '20140601_444_profile-%s' % func_name, u'depgrp4'),
         (app1, '20140601_876_profile-%s' % func_name, u'depgrp3'),
         ]
    )

    # test the filter_deps option
    nt.assert_items_equal(
        list(dag_tools.get_parents(
            depends_on1, '20140601_testID3-%s' % func_name, True,
            filter_deps=['depgrp4'])),
        [(app1, '20140601_444_profile-%s' % func_name, u'depgrp4')]
    )

    with nt.assert_raises(exceptions.DAGMisconfigured):
        list(dag_tools.get_parents(
            depends_on1, '20140601_testID3-%s' % func_name, True,
            filter_deps=['depgrp99999']))


@tt.with_setup
def test_fan_out_tasks(app1, app2, app4, fanout1, func_name):
    # test for Many-to-Many relationships between parent and child tasks
    nt.assert_items_equal(
        list(dag_tools.get_parents(
            'test_stolos/test_fan_out_tasks/fanout1', '20140715_8')),
        [])

    nt.assert_items_equal(
        list(dag_tools.get_parents(
            'test_stolos/test_fan_out_tasks/fanout1',
            '20140715_testID5-%s' % func_name, True)),
        [
            (app1, '20140714_555_profile-%s' % func_name, u'dep2'),
            (app1, '20140715_555_profile-%s' % func_name, u'dep2'),
        ])

    nt.assert_items_equal(
        list(dag_tools.get_children(
            'test_stolos/test_fan_out_tasks/app1',
            '20140715_9_profile-%s' % func_name, True,)),
        [(app2, '20140715_9_profile-%s' % func_name, 'default'),
         (app4, '20140715_9_profile-%s' % func_name, 'default'),
         (fanout1, '20140715_testID1-%s' % func_name, u'dep1'),
         (fanout1, '20140715_testID2-%s' % func_name, u'dep1'),
         (fanout1, '20140715_testID3-%s' % func_name, u'dep1'),
         ])

    nt.assert_items_equal(
        list(dag_tools.get_children(
            app1, '20140715_555_profile-%s' % func_name, True,)),
        [
            (app2, '20140715_555_profile-%s' % func_name, 'default'),
            (app4, '20140715_555_profile-%s' % func_name, 'default'),
            (fanout1, u'20140714_testID5-%s' % func_name, u'dep2'),
            (fanout1, u'20140714_testID6-%s' % func_name, u'dep2'),
            (fanout1, u'20140715_testID1-%s' % func_name, u'dep1'),
            (fanout1, u'20140715_testID2-%s' % func_name, u'dep1'),
            (fanout1, u'20140715_testID3-%s' % func_name, u'dep1'),
            (fanout1, u'20140715_testID5-%s' % func_name, u'dep2'),
            (fanout1, u'20140715_testID6-%s' % func_name, u'dep2'),
        ])


@tt.with_setup
def test_topological_sort(topological_sort1, app1, app2, depends_on1, bash2,
                          depends_on_job_id1, func_name):
    nt.assert_items_equal(
        list(dag_tools.topological_sort(dag_tools.get_parents(
            topological_sort1, depends_on_job_id1, True,))),
        [
            (app1, '20140601_101_profile-%s' % func_name, u'dep1'),
            (app1, '20140601_102_profile-%s' % func_name, u'dep1'),
            (app2, '20140601_101_profile-%s' % func_name, u'dep1'),
            (app2, '20140601_102_profile-%s' % func_name, u'dep1'),
            (depends_on1, u'20140601_testID1-%s' % func_name, u'dep1'),
            (bash2, '20140601_101_profile-%s' % func_name, u'dep1'),
            (bash2, '20140601_102_profile-%s' % func_name, u'dep1')
        ]
    )


@tt.with_setup
def test_depends_on_all(func_name, all_test1, all_test2, all_test3, all_test4,
                        all_test4b, all_test5):
    # all_test1 children
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test1, '20140601_1', True,)),
        [
            (all_test3, '20140601_1', 'default'),
            (all_test3, '20140601_2', 'default'),
            (all_test4, '20140601', 'default'),
            (all_test4b, '20140601', 'both'),
            (all_test5, '20140601', 'both_apps'),
        ])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test1, '20140601_1', True,)),
        list(dag_tools.get_children(
            all_test1, '20140601_2', True,))
    )
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test1, '20140601_0', True,)),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test1, '20140601_0', True,)),
        [])

    # all_test2 children
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test2, '20140601_0', True,)),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test2, '20140601_1', True,)),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test2, '20140601_2', True,)),
        [
            (all_test4, '20140601', 'default'),
            (all_test4b, '20140601', 'both')
        ])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test2, '20140601_3', True,)),
        [
            (all_test4, '20140601', 'default'),
            (all_test4b, '20140601', 'both')
        ])
    nt.assert_items_equal(
        list(dag_tools.get_children(
            all_test2, '20140601_4', True,)),
        [(all_test5, '20140601', 'both_apps')])

    # all_test3 parents
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test3, "20140601_0", True)),
        [])
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test3, "20140601_1", True)),
        [
            (all_test1, '20140601_1', 'default'),
            (all_test1, '20140601_2', 'default'),
        ])
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test3, "20140601_2", True)),
        list(dag_tools.get_parents(all_test3, "20140601_1", True)),
    )
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test3, "20140601_3", True)),
        [])

    # all_test4 parents
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test4, "20140601", True)),
        [
            (all_test1, '20140601_1', 'default'),
            (all_test1, '20140601_2', 'default'),
            (all_test2, '20140601_2', 'default'),
            (all_test2, '20140601_3', 'default'),
        ])
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test4, "20140601", True)),
        list(dag_tools.get_parents(all_test4, "20140601", True)),
    )

    # all_test5 parents
    nt.assert_items_equal(
        list(dag_tools.get_parents(all_test5, "20140601", True)),
        [
            (all_test1, '20140601_1', 'both_apps'),
            (all_test1, '20140601_2', 'both_apps'),
            (all_test2, '20140601_4', 'both_apps')
        ])
