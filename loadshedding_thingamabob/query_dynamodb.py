import datetime
import logging
import pprint

import boto3
import urllib.request

import scraping.scraping
import database.dynamodb

def query_recent(table_name: str, region_loadshedding: str, suffix: str, table=None):
    logger = logging.getLogger()

    if not table:
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

    return timestamp_recent, data_recent
