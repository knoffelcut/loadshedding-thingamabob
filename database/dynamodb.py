
import boto3

class EmptyDynamoResponse(Exception):
    pass

def get_most_recent_scraped_data(table, primary_key: str):
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('region').eq(primary_key), ScanIndexForward=False, Limit=1
        )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    items = response['Items']

    if not items:
        raise EmptyDynamoResponse(f"No items in table='{table.name}' matching primary_key='{primary_key}'")

    assert len(items) == 1

    item = items[0]
    assert item['region'] == primary_key

    timestamp = int(item['timestamp'])

    data = item['data']

    return timestamp, data
