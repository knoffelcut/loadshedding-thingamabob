import os
import datetime
import pprint
import tempfile

import boto3

import loadshedding_coct_stage_query.schedule

# From https://stackoverflow.com/a/12809659
#  and https://blog.yadutaf.fr/2012/10/07/common-dynamodb-questionsmisconceptionsrecipes/
def main(table_name: str, region: str):
    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    # Query(hash_key=..., ScanIndexForward=True, limit=1)
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('region').eq(region), ScanIndexForward=False, Limit=1
        )
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    items = response['Items']
    assert len(items) == 1

    item = items[0]
    assert item['region'] == region

    timestamp = int(item['timestamp'])
    print(f'Timestamp = {timestamp} ({datetime.datetime.fromtimestamp(timestamp).isoformat()})')

    with tempfile.NamedTemporaryFile('w', delete=False) as f:
        f.write(item['data'])

    schedule = loadshedding_coct_stage_query.schedule.Schedule.from_file(f.name)
    print(schedule)

    os.remove(f.name)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Upload a schedule to boto3')
    parser.add_argument('--table_name', type=str, default='loadshedding-schedules',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region', type=str, default='coct',
        help='Schedule region.'
        )
    args = parser.parse_args()

    main(**vars(args))
