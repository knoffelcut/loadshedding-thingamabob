import datetime
import pprint

import boto3

def main(path: str, table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime):
    timestamp = int(date.timestamp())
    with open(path) as f:
        data = f.read()

    partition_key = f"{region_loadshedding}-{suffix}"

    item = {
        'field': partition_key,
        'timestamp': timestamp,
        'data': data,
    }

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)
    response = table.put_item(
        Item=item
    )

    pprint.pprint(response)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Upload a schedule to boto3')
    parser.add_argument('--path', type=str, default='schedules/test0.csv',
        help='Path to ".csv" schedule file.'
        )
    parser.add_argument('--table_name', type=str, default='loadshedding',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region_loadshedding', type=str, default='coct',
        help='Schedule region.'
        )
    parser.add_argument('--date', type=datetime.datetime.fromisoformat, default=datetime.datetime.now().isoformat(),
        help='Datetime used as the primary key.'
        )
    args = parser.parse_args()

    main(
        **vars(args),
        suffix='loadshedding-schedule-csv'
        )
