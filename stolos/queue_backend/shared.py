from os.path import join
from stolos import get_NS


# All possible job states, as stored in the queue backend
PENDING = 'pending'
COMPLETED = 'completed'
FAILED = 'failed'
SKIPPED = 'skipped'


def get_job_path(app_name, job_id, *args):
    return join(app_name, 'all_subtasks', job_id, *args)


def get_lock_path(typ, app_name, job_id):
    assert typ in set(['execute', 'add'])
    return join(get_job_path(app_name, job_id), '%s_lock' % typ)


def get_qbclient():
    return get_NS().queue_backend
