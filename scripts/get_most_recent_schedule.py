import pathlib
import argparse
import datetime
import pprint
import json

import utility.lambda_helper
import utility.logger
import loadshedding_thingamabob.query_dynamodb
import loadshedding_thingamabob.schedule

def get_parser():
    parser = argparse.ArgumentParser(description='Prints the most recent scraped data')
    parser.add_argument('--table_name', type=str, default='loadshedding',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region_loadshedding', type=str, default='coct',  # National
        help='Loadshedding Stage Schedule region.'
        )

    return parser

def main(args: argparse.Namespace):
    timestamp_recent, data_recent = loadshedding_thingamabob.query_dynamodb.query_recent(
        **vars(args),
        suffix='loadshedding-schedule-csv',
        )

    return timestamp_recent, data_recent

if __name__ == '__main__':
    logger = utility.logger.setup_logger_cli(pathlib.PurePath(__file__).stem)

    parser = get_parser()
    args = parser.parse_args()
    timestamp_recent, data_recent = main(args)

    schedule = loadshedding_thingamabob.schedule.Schedule.from_string(data_recent)

    logger.info(f'Timestamp Recent: {timestamp_recent} ({datetime.datetime.fromtimestamp(timestamp_recent).isoformat()})')
    logger.info(f'Data Recent:\n{schedule}')

def lambda_handler(event: dict, context):
    utility.logger.setup_logger_lambda()

    parser = get_parser()
    args = utility.lambda_helper.parse_events_as_args(parser, event)
    timestamp_recent, data_recent = main(args)

    return {
        "statusCode": 200,
        "body": json.dumps({
            'timestamp': timestamp_recent,
            'schedule_csv': data_recent
        }),
    }
