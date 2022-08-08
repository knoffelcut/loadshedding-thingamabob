
import boto3


class EmptyDynamoResponse(Exception):
    pass


def get_recent_scraped_data(table, primary_key: str, n_entries: int = 1, strict: bool = False):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('field').eq(primary_key), ScanIndexForward=False, Limit=n_entries
    )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    items = response['Items']

    if not items:
        raise EmptyDynamoResponse(
            f"No items in table='{table.name}' matching primary_key='{primary_key}'")

    assert (not strict) or len(items) == n_entries

    return items

# From https://stackoverflow.com/a/12809659
#  and https://blog.yadutaf.fr/2012/10/07/common-dynamodb-questionsmisconceptionsrecipes/


def get_most_recent_scraped_data(table, primary_key: str):
    items = get_recent_scraped_data(table, primary_key, 1, True)

    item = items[0]
    assert item['field'] == primary_key

    timestamp = int(item['timestamp'])

    data = item['data']

    return timestamp, data
