"""
An example job_id_validations file for Stolos.
In this file, you define the components of job_ids.
You can also produce side-effects like configure Stolos's logging
"""
import logging
import datetime as dt

# Example side-effect: Configure logging for Stolos
from stolos.api import configure_logging
log = configure_logging(add_handler=True, colorize=True)
log.setLevel(logging.INFO)


# Example job_id validations

# These validations help Stolos parse your job_ids into component parts and
# verify that they are of acceptable types


def is_valid_date(date):
    year = int(str(date)[:4])
    month = int(str(date)[4:6])
    day = int(str(date)[6:])
    return bool(dt.date(year, month, day)) and int(date)


def to_int(inpt):
    return int(inpt)


def is_valid_collection_name(collection_name):
    return (
        any(collection_name.startswith(x) for x in set(
            ['client', 'profile', 'purchase', 'content'])) and
        collection_name)


def to_str(inpt):
    if isinstance(inpt, bytes):
        inpt = inpt.decode('utf8')
    return str(inpt)


JOB_ID_VALIDATIONS = {
    'date': is_valid_date,
    'client_id': to_int,
    'collection_name': is_valid_collection_name,
    'testID': to_str,
}
