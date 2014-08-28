import logging
import datetime as dt


JOB_ID_VALIDATIONS = {
    'date': lambda date: bool(
        dt.date(int(str(date)[:4]), int(str(date)[4:6]), int(str(date)[6:]))
    ) and int(date),
    'client_id': lambda client_id: int(client_id),
    'collection_name': lambda collection_name: (bool(
        collection_name in set(['client', 'profile', 'purchase', 'content']))
        and collection_name),
    'testID': lambda test_id: int(test_id)
}

# configure logging for tests
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
