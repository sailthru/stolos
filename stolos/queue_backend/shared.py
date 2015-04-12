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


# TODO: figure out where this goes
def get_qsize(app_name, queued=True, taken=True):
    """
    Find the number of jobs in the given app's queue

    `queued` - Include the entries in the queue that are not currently
        being processed or otherwise locked
    `taken` - Include the entries in the queue that are currently being
        processed or are otherwise locked
    """
    qb = get_NS().queue_backend()
    pq = join(app_name, 'entries')
    pt = join(app_name, 'taken')
    if queued:
        entries = len(qb.get_children(pq))
        if taken:
            return entries
        else:
            taken = len(qb.get_children(pt))
            return entries - taken
    else:
        if taken:
            taken = len(qb.get_children(pt))
            return taken
        else:
            raise AttributeError(
                "You asked for an impossible situation.  Queue items are"
                " waiting for a lock xor taken.  You cannot have queue entries"
                " that are both not locked and not waiting.")
