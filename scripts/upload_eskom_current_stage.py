import pathlib
import argparse
import datetime

import scraping.scraping
import loadshedding_thingamabob.query_and_upload

import utility.lambda_helper
import utility.logger

def get_parser():
    parser = argparse.ArgumentParser(description='Uploads the current national loadshedding stage as reported by Eskom')
    parser.add_argument('--url', type=str, default='http://loadshedding.eskom.co.za/LoadShedding/GetStatus',
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

    return parser

def main(args: argparse.Namespace):
    return loadshedding_thingamabob.query_and_upload.query_and_upload(
        **vars(args),
        suffix='current-stage',
        f_scrape=scraping.scraping.extract_eskom_loadshedding_stage,
        f_datapack=lambda x: x
        )

if __name__ == '__main__':
    utility.logger.setup_logger_cli(pathlib.PurePath(__file__).stem)

    parser = get_parser()
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
