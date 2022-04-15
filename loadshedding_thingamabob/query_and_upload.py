import datetime
import logging
import pprint

import boto3
import urllib.request

import scraping.scraping
import database.dynamodb

def query_and_upload(url: str, table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime, attempts, f_scrape, f_datapack):
    logger = logging.getLogger()

    while True:
        try:
            with urllib.request.urlopen(url) as response_dynamodb:
                # TODO Retry like 5 times
                assert response_dynamodb.status == 200

                html = response_dynamodb.read()

                data = f_scrape(html)
        except (AssertionError, scraping.scraping.ScrapeError) as e:
            # TODO Send email via SNS
            if isinstance(e, AssertionError):
                logger.error(f'HTML Request Failed\nResponse: {response_dynamodb}')
            elif isinstance(e, scraping.scraping.ScrapeError):
                logger.error(f'Scraping Response Failed\nException: {str(e)}')
                logger.error(f'Raw scraped data: {str(html)}')

            if attempts > 0:
                attempts -= 1
                continue
            else:
                raise e

        break
    data = f_datapack(data)

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    partition_key = f"{region_loadshedding}-{suffix}"

    try:
        timestamp_recent, data_recent = database.dynamodb.get_most_recent_scraped_data(table, partition_key)
        logger.info(f'Timestamp Recent = {timestamp_recent} ({datetime.datetime.fromtimestamp(timestamp_recent).isoformat()})')
        logger.info(f'Data Recent =      {data_recent}')
    except database.dynamodb.EmptyDynamoResponse as e:
        timestamp_recent, data_recent = None, None
        logger.warning(
            f'DynamoDB table is empty for partition key: {partition_key}'
            f'Exception: {e}'
            )

    if data_recent is None or data != data_recent:
        # Upload the data to dynamodb
        logger.info('Uploading data to database')
        timestamp = int(date.timestamp())
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
        assert response_dynamodb['ResponseMetadata']['HTTPStatusCode'] == 200
        assert response_sns['ResponseMetadata']['HTTPStatusCode'] == 200
    else:
        logger.info('Scraped data is identical to most recent data. Skipping upload')
