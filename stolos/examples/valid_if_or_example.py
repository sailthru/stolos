import datetime


def func(app_name, **parsed_job_id):
    """
    This example function evaluates the identifiers in a job_id to decide
    whether a particular job should be executed or not.  To use it, your task
    configuration must define a "valid_if_or" section that points to this
    function.

    This functionality is useful when you don't wish to create a new identifier
    for a particular job_id or you wish to have Stolos mark specific
    job_ids as "skipped"

    PARAMS:
    app_name - the task name.  We provide this option so this func can be
        generalized to more than one scheduled app
    **parsed_job_id - specifies the identifiers that make up the job_id.
        You could also just decide to explicitly define keyword args like:
            def func(app_name, date, client_id, collection_name)

    In this particular example, this function will not let Stolos queue
    any job_ids except those job_ids where client_id is 1111 where the given
    date is a Wednesday or Sunday.  All other job_ids this function receives
    will not be queued by Stolos, and will instead be marked as
    "skipped"

    """
    c1 = parsed_job_id['client_id'] == 1111

    _day_of_week = datetime.datetime.strptime(
        str(parsed_job_id['date']), '%Y%m%d').strftime("%A")
    c2 = _day_of_week in ['Friday', 'Sunday']
    from stolos.examples import log
    log.critical('%s %s %s %s' % (app_name, parsed_job_id, c1, c2))
    if c1 and c2:
        return True
    else:
        return False
