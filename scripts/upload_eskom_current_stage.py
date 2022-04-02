import datetime
import pprint

import boto3
import urllib.request

import scraping.scraping
import database.dynamodb

def main(url: str, table_name: str, region_loadshedding: str, date: datetime.datetime, retry, f_scrape, f_datapack):
    with urllib.request.urlopen(url) as response:
        try:
            # TODO Retry like 5 times
            assert response.status == 200
        except AssertionError as e:
            # TODO Send email via SNS
            print(f'HTML Request Failed')
            pprint.pprint(response)
            raise e

        html = response.read()

    primary_key = f"{region_loadshedding}-{'plaintext'}"

    data = f_scrape(html)
    data = f_datapack(data)

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    try:
        timestamp_recent, data_recent = database.dynamodb.get_most_recent_scraped_data(table, primary_key)
        print(f'Timestamp Recent = {timestamp_recent} ({datetime.datetime.fromtimestamp(timestamp_recent).isoformat()})')
        print(f'Data Recent =      {data_recent}')
    except database.dynamodb.EmptyDynamoResponse as e:
        timestamp_recent, data_recent = None, None
        print(f'Handled exception:\n{e}')
        print(f'No data is available')

    if data_recent is None or data != data_recent:
        # Upload the data to dynamodb
        print('Uploading data to database')
        timestamp = int(date.timestamp())
        item = {
            'region': primary_key,
            'timestamp': timestamp,
            'data': data,
        }

        response = table.put_item(
            Item=item
        )

        pprint.pprint(response)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    else:
        print('Scraped data is identical to most recent data. Skipping upload')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Uploads the current national loadshedding stage as reported by Eskom')
    parser.add_argument('--url', type=str, default='http://loadshedding.eskom.co.za/LoadShedding/GetStatus',
        help='Eskom API URL to get the current stage'
        )
    parser.add_argument('--table_name', type=str, default='loadshedding-schedules',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region_loadshedding', type=str, default='national',
        help='Loadshedding Stage Schedule region.'
        )
    parser.add_argument('--date', type=datetime.datetime.fromisoformat, default=datetime.datetime.now().isoformat(),
        help='Datetime used as the sort key.'
        )
    parser.add_argument('--retry', type=int, default=5,
        help='Number of times to retry the HTTP call.'
        )
    args = parser.parse_args()

    main(**vars(args), f_scrape=scraping.scraping.extract_eskom_loadshedding_stage, f_datapack=lambda X: X)
