import pathlib
import argparse
import datetime

import utility.lambda_helper
import utility.logger
import loadshedding_thingamabob.query_dynamodb

def get_parser():
    parser = argparse.ArgumentParser(description='Prints the most recent plaintext data')
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
        suffix='plaintext',
        )

    return timestamp_recent, data_recent

if __name__ == '__main__':
    logger = utility.logger.setup_logger_cli(pathlib.PurePath(__file__).stem)

    parser = get_parser()
    args = parser.parse_args()
    timestamp_recent, data_recent = main(args)

    logger.info(f'Timestamp Recent: {timestamp_recent} ({datetime.datetime.fromtimestamp(timestamp_recent).isoformat()})')
    logger.info(f'Data Recent:\n{data_recent}')

def lambda_handler(event: dict, context):
    utility.logger.setup_logger_lambda()

    parser = get_parser()
    args = utility.lambda_helper.parse_events_as_args(parser, event)
    main(args)

    raise NotImplementedError

    return {
        "statusCode": 200,
        "body": '{}',
    }
