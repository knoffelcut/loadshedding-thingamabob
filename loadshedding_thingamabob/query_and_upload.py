import datetime
import logging
import pprint
from typing import Callable

import boto3
import urllib.request

import loadshedding_thingamabob.query_dynamodb


class ValidationException(Exception):
    pass


def print_response_url(response_url):
    return f'code: {response_url.status}\nurl: {response_url.url}\msg: {response_url.msg}\reason: {response_url.reason}'


def query_and_upload(
        url: str, table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime, attempts: int,
        f_validate: Callable, f_datapack: Callable,
        sns_notify: bool, database_write: bool):
    """_summary_
    TODO Document

    TODO The script(s) that call this function is a bit of a monolith as a Lambda function, and needs to be split into
        3 Lambdas:
            1. URL Request + DB Request, delta check
            2. Upload to plaintext
            3. Parse and upload schedule

    Args:
        url (str): _description_
        table_name (str): _description_
        region_loadshedding (str): _description_
        suffix (str): _description_
        date (datetime.datetime): _description_
        attempts (int): _description_
        f_validate (Callable): _description_
        f_datapack (Callable): _description_
        sns_notify (bool): _description_
        database_write (bool): _description_

    Raises:
        e: _description_

    Returns:
        _type_: _description_
    """
    logger = logging.getLogger()

    while True:
        try:
            with urllib.request.urlopen(url) as response_url:
                assert response_url.status == 200

                data = response_url.read()

            f_validate(data)

            break
        except (AssertionError, ValidationException) as e:
            if isinstance(e, AssertionError):
                logger.error(
                    f'HTML Request Failed\nResponse: {print_response_url(response_url)}\nResponse Status: {response_url.status}')
                logger.exception(e)
            if isinstance(e, ValidationException):
                logger.error(
                    f'Validation Failed\nResponse: {print_response_url(response_url)}')
                logger.exception(e)

            attempts -= 1
            logger.error(f'Attempts left: {attempts}')

            if attempts <= 0:
                raise e

    timestamp = int(date.timestamp())
    data = f_datapack(data)
    logger.info(
        f'Timestamp = {timestamp} ({datetime.datetime.fromtimestamp(timestamp).isoformat()})')
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
                    'data_recent': data_recent,
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

        return timestamp, data, True
    else:
        logger.info(
            'Scraped data is identical to most recent data. Skipping upload')

        return timestamp, data, False


def convert_plaintext_and_upload(
    timestamp: int, plaintext: str,
    table_name: str, region_loadshedding: str, suffix: str, date: datetime.datetime, attempts: int,
    f_convert: Callable,
    sns_notify: bool, database_write: bool,
    **kwargs,
):
    """_summary_
    TODO Document

    Args:
        data (str): _description_
        table_name (str): _description_
        region_loadshedding (str): _description_
        suffix (str): _description_
        date (datetime.datetime): _description_
        attempts (int): _description_
        f_convert (Callable): _description_
        sns_notify (bool): _description_
        database_write (bool): _description_

    Raises:
        e: _description_

    Returns:
        _type_: _description_
    """
    logger = logging.getLogger()

    schedule = f_convert(timestamp, plaintext)
    logger.info(schedule)

    dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
    table = dynamodb.Table(table_name)

    partition_key = f"{region_loadshedding}-{suffix}"

    # Upload the data to dynamodb
    data = schedule.to_csv()

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
                'table_name': table_name,
                'region_loadshedding': region_loadshedding,
            }, indent=4),
            Subject='Loadshedding Schedule Updated'
        )

        logger.info(f'SNS response: {response_sns}')

    # Some assertions
    if database_write:
        assert response_dynamodb['ResponseMetadata']['HTTPStatusCode'] == 200
    if sns_notify:
        assert response_sns['ResponseMetadata']['HTTPStatusCode'] == 200

    return True
