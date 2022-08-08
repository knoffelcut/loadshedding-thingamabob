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
    parser = argparse.ArgumentParser(
        description='Prints the most recent scraped data')
    parser.add_argument('--table_name', type=str, default='loadshedding',
                        help='DynamoDB Table Name.'
                        )
    parser.add_argument('--region_loadshedding', type=str, default='national',  # National
                        help='Loadshedding Stage Schedule region.'
                        )

    return parser


def main(args: argparse.Namespace):
    timestamp_recent, data_recent = loadshedding_thingamabob.query_dynamodb.query_recent(
        **vars(args),
        suffix='schedule',
    )

    return timestamp_recent, data_recent


if __name__ == '__main__':
    logger = utility.logger.setup_logger_cli(pathlib.PurePath(__file__).stem)

    parser = get_parser()
    args = parser.parse_args()
    timestamp, data = main(args)

    schedule = loadshedding_thingamabob.schedule.Schedule.from_string(data)

    logger.info(
        f'Timestamp Recent: {timestamp} ({datetime.datetime.fromtimestamp(timestamp).isoformat()})')
    logger.info(f'CSV:\n{data}\n')
    logger.info('CSV:\n' + '\n'.join(
        [f"{datetime.datetime.fromtimestamp(int(l.split(', ')[0]))}, {l.split(', ')[1]}" for l in data.split('\n')]) + '\n')
    logger.info(f'Schedule:\n{schedule}\n')


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
