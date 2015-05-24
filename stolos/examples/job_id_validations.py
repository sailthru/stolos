import logging
import datetime as dt

# configure logging for tests
from stolos.api import configure_logging
log = configure_logging(add_handler=True, colorize=True)
log.setLevel(logging.INFO)


JOB_ID_VALIDATIONS = {
    'date': lambda date: bool(
        dt.date(int(str(date)[:4]), int(str(date)[4:6]), int(str(date)[6:]))
    ) and int(date),
    'client_id': lambda client_id: int(client_id),
    'collection_name': lambda collection_name: (
        any(collection_name.startswith(x) for x in set(
            ['client', 'profile', 'purchase', 'content'])) and collection_name
    ),
    'testID': lambda test_id: str(test_id)
}
