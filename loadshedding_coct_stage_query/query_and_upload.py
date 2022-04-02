import datetime
import pprint

import boto3
import urllib.request

import scraping.scraping
import database.dynamodb

def query_and_upload(url: str, table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime, attempts, f_scrape, f_datapack):
    while True:
        try:
            with urllib.request.urlopen(url) as response:
                # TODO Retry like 5 times
                assert response.status == 200

                html = response.read()
        except AssertionError as e:
            # TODO Send email via SNS
            print(f'HTML Request Failed')
            pprint.pprint(response)

            if attempts > 0:
                attempts -= 1
                continue
            else:
                raise e

        break

    partition_key = f"{region_loadshedding}-{suffix}"

    data = f_scrape(html)
    data = f_datapack(data)

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    try:
        timestamp_recent, data_recent = database.dynamodb.get_most_recent_scraped_data(table, partition_key)
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
            'field': partition_key,
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
