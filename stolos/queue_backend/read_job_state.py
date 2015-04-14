from stolos import exceptions

from . import shared


def validate_state(
        pending, completed, failed, skipped, all=False, multi=False):
    """Helper function to that raises UserWarning if user's request defines
    an invalid combination of job states
    """
    cnt = pending + completed + failed + skipped
    if multi:
        if cnt < 1 and not all:
            raise UserWarning(
                "you must request at least one of these states:"
                " pending, completed, failed, skipped")
    else:
        if cnt != 1:
            raise UserWarning(
                "you must request exactly one of these states:"
                " pending, completed, failed, skipped")
    rv = []
    if all or pending:
        rv.append(shared.PENDING)
    if all or completed:
        rv.append(shared.COMPLETED)
    if all or failed:
        rv.append(shared.FAILED)
    if all or skipped:
        rv.append(shared.SKIPPED)
    if multi:
        return rv
    else:
        return rv[0]


def check_state(app_name, job_id, raise_if_not_exists=False,
                pending=False, completed=False, failed=False, skipped=False,
                all=False, _get=False):
    """Determine whether a specific job is in one or more specific state(s)

    If job_id is a string, return a single value.
    If multiple job_ids are given, return a list of values

    `app_name` is a task identifier
    `job_id` (str or list of str) is a subtask identifier or a list of them
    `all` (bool) if True, return True if the job_id is in a recognizable state
    `_get` (bool) if True, just return the string value of the state and
                  ignore the (pending, completed, xor failed) choice
    """
    qbcli = shared.get_qbclient()
    if isinstance(job_id, (str, unicode)):
        job_ids = [job_id]
        rvaslist = False
    else:
        job_ids = job_id
        rvaslist = True

    rv = []
    for job_id in job_ids:
        job_path = shared.get_job_path(app_name, job_id)
        try:
            gotstate = qbcli.get(job_path)
        except exceptions.NoNodeError:
            if raise_if_not_exists:
                raise
            else:
                rv.append(False)
                continue
        if _get:
            rv.append(gotstate)
            continue
        else:
            accepted_states = validate_state(
                pending, completed, failed, skipped, all=all, multi=True)
            rv.append(gotstate in accepted_states)
            continue
    if rvaslist:
        return rv
    else:
        return rv[0]
