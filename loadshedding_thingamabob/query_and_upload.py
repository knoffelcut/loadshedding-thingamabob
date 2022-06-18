import datetime
import logging
import pprint
from typing import Callable

import boto3
import urllib.request

import loadshedding_thingamabob.query_dynamodb

def query_and_upload(
    url: str, table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime, attempts: int,
    f_datapack: Callable,
    sns_notify: bool, database_write: bool):
    logger = logging.getLogger()

    while True:
        try:
            with urllib.request.urlopen(url) as response_url:
                # TODO Retry like 5 times
                assert response_url.status == 200

                data = response_url.read()

            break
        except (AssertionError) as e:
            if isinstance(e, AssertionError):
                logger.error(f'HTML Request Failed\nResponse: {response_url}')

            if attempts > 0:
                attempts -= 1
                continue
            else:
                # TODO Send email via SNS
                raise e

    timestamp = int(date.timestamp())
    data = f_datapack(data)
    logger.info(f'Timestamp = {timestamp} ({datetime.datetime.fromtimestamp(timestamp).isoformat()})')
    logger.info(f'Data =      {data}')

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    timestamp_recent, data_recent = loadshedding_thingamabob.query_dynamodb.query_recent(
        None, region_loadshedding, suffix, table
        )

    partition_key = f"{region_loadshedding}-{suffix}"

    if data_recent is None or data != data_recent:
        logger.info('Scraped data differs from most recent data')

        # Upload the data to dynamodb
        if database_write:
            logger.info('Uploading data to database')
            item = {
                'field': partition_key,
                'timestamp': timestamp,
                'data': data,
            }

            response_dynamodb = table.put_item(
                Item=item
            )

            logger.info(f'dynamodb response: {response_dynamodb}')

        # Publish change via SNS
        if sns_notify:
            logger.info('Publishing Change via SNS')
            client_sns = boto3.client('sns', region_name='af-south-1')
            response_sns = client_sns.publish(
                TopicArn='arn:aws:sns:af-south-1:273749684738:loadshedding-deltas',
                Message=pprint.pformat({
                    'data': data,
                    'url': url,
                    'table_name': table_name,
                    'region_loadshedding': region_loadshedding,
                }, indent=4),
                Subject='Loadshedding Scraper delta detected'
            )

            logger.info(f'SNS response: {response_sns}')

        # Some assertions
        if database_write:
            assert response_dynamodb['ResponseMetadata']['HTTPStatusCode'] == 200
        if sns_notify:
            assert response_sns['ResponseMetadata']['HTTPStatusCode'] == 200

        return data
    else:
        logger.info('Scraped data is identical to most recent data. Skipping upload')

        return None
