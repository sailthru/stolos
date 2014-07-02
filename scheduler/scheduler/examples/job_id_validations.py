import datetime as dt


JOB_ID_VALIDATIONS = {
    'date': lambda date: bool(
        dt.date(int(date[:4]), int(date[4:6]), int(date[6:]))) and int(date),
    'client_id': lambda client_id: int(client_id),
    # TODO: I've hardcoded this for now
    'collection_name': lambda collection_name: (bool(
        collection_name in set(['client', 'profile', 'purchase', 'content']))
        and collection_name),
    'testID': lambda test_id: int(test_id)
}
