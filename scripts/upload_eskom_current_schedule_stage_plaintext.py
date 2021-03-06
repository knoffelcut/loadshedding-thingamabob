import pathlib
import argparse
import datetime

import loadshedding_thingamabob.query_and_upload

import utility.lambda_helper
import utility.logger

def get_parser():
    parser = argparse.ArgumentParser(description=
        'Uploads the national loadshedding schedule from the public Eskom API. '
        'Only the current stage if available from the API.'
        )
    parser.add_argument('--url', type=str, default='https://loadshedding.eskom.co.za/LoadShedding/GetStatus',
        help='Eskom API URL to get the current stage'
        )
    parser.add_argument('--table_name', type=str, default='loadshedding',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region_loadshedding', type=str, default='national',
        help='Loadshedding Stage Schedule region.'
        )
    parser.add_argument('--date', type=datetime.datetime.fromisoformat, default=datetime.datetime.now().isoformat(),
        help='Datetime used as the sort key.'
        )
    parser.add_argument('--attempts', type=int, default=5,
        help='Number of times to attempt the HTTP call.'
        )
    parser.add_argument('--sns_notify', type=bool, default=True,
        help='If different than recent, send an AWS SNS notification.'
        )
    parser.add_argument('--database_write', type=bool, default=True,
        help='If scraped data differs from most recent data, write changes to database.'
        )

    return parser

def f_validate(data):
    try:
        int(data)
    except Exception as e:
        raise loadshedding_thingamabob.query_and_upload.ValidationException from e

def main(args: argparse.Namespace):
    return loadshedding_thingamabob.query_and_upload.query_and_upload(
        **vars(args),
        suffix='plaintext',
        f_validate=f_validate,
        f_datapack=lambda x: x.decode()
        )

if __name__ == '__main__':
    utility.logger.setup_logger_cli(pathlib.PurePath(__file__).stem)

    parser = get_parser()
    parser.set_defaults(
        sns_notify=False,
        database_write=False,
    )
    args = parser.parse_args()
    main(args)

def lambda_handler(event: dict, context):
    utility.logger.setup_logger_lambda()

    parser = get_parser()
    args = utility.lambda_helper.parse_events_as_args(parser, event)
    main(args)

    return {
        "statusCode": 200,
        "body": '{}',
    }
