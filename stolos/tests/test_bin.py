import os
from subprocess import check_output, CalledProcessError
from nose import tools as nt
from stolos import queue_backend as qb

from stolos.testing_tools import (
    with_setup, validate_zero_queued_task, validate_one_queued_task,
    validate_n_queued_task
)


def run(cmd, tasks_json_tmpfile, **kwargs):
    cmd = (
        "set -o pipefail ; STOLOS_TASKS_JSON={tasks_json} {cmd}").format(
            cmd=cmd, tasks_json=tasks_json_tmpfile, **kwargs)
    rv = check_output(cmd, shell=True, executable="bash", env=os.environ)
    return rv


@with_setup
def test_stolos_submit(app1, job_id1, tasks_json_tmpfile):
    with nt.assert_raises(CalledProcessError):
        run("stolos-submit -h", tasks_json_tmpfile)
    validate_zero_queued_task(app1)
    run("stolos-submit -a %s -j %s" % (app1, job_id1), tasks_json_tmpfile)
    validate_one_queued_task(app1, job_id1)
    run("stolos-submit -a %s -j %s" % (app1, job_id1), tasks_json_tmpfile)
    validate_one_queued_task(app1, job_id1)


@with_setup
def test_stolos_submit_readd(app1, job_id1, tasks_json_tmpfile):
    qb.set_state(app1, job_id1, failed=True)
    validate_zero_queued_task(app1)
    run("stolos-submit -a %s -j %s" % (app1, job_id1),
        tasks_json_tmpfile)
    validate_zero_queued_task(app1)
    run("stolos-submit -a %s -j %s --readd" % (app1, job_id1),
        tasks_json_tmpfile)
    validate_one_queued_task(app1, job_id1)


@with_setup
def test_stolos_submit_multiple_jobs(app1, app2, job_id1, job_id2,
                                     tasks_json_tmpfile):
    validate_zero_queued_task(app1)
    validate_zero_queued_task(app2)
    run("stolos-submit -a %s %s -j %s %s" % (app1, app2, job_id1, job_id2),
        tasks_json_tmpfile)
    validate_n_queued_task(app1, job_id1, job_id2)
    validate_n_queued_task(app2, job_id1, job_id2)
    run("stolos-submit -a %s %s -j %s %s" % (app1, app2, job_id1, job_id2),
        tasks_json_tmpfile)
    validate_n_queued_task(app1, job_id1, job_id2)
    validate_n_queued_task(app2, job_id1, job_id2)
