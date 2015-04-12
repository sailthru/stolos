PENDING = 'pending'
COMPLETED = 'completed'
FAILED = 'failed'
SKIPPED = 'skipped'


def validate_state(pending, completed, failed, skipped,
                    all=False, multi=False):
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
        rv.append(state.PENDING)
    if all or completed:
        rv.append(state.COMPLETED)
    if all or failed:
        rv.append(state.FAILED)
    if all or skipped:
        rv.append(state.SKIPPED)
    if multi:
        return rv
    else:
        return rv[0]


