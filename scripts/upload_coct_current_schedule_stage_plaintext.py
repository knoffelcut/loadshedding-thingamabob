
import pathlib
import argparse
import datetime

import scraping.scraping
import loadshedding_thingamabob.query_and_upload

import utility.lambda_helper
import utility.logger

def get_parser():
    parser = argparse.ArgumentParser(description='Uploads the loadshedding schedule specific section of the CoCT website to DynamoDB')
    parser.add_argument('--url', type=str, default='https://www.capetown.gov.za/Family%20and%20home/residential-utility-services/residential-electricity-services/load-shedding-and-outages',
        help='Path to the CoCT website with loadshedding information'
        )
    parser.add_argument('--table_name', type=str, default='loadshedding',
        help='DynamoDB Table Name.'
        )
    parser.add_argument('--region_loadshedding', type=str, default='coct',
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

def main(args: argparse.Namespace):
    return loadshedding_thingamabob.query_and_upload.query_and_upload(
        **vars(args),
        suffix='scraped',
        f_scrape=scraping.scraping.extract_coct_loadshedding_text,
        f_datapack=lambda data: '\n'.join(data)
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

def lambda_handler(event, context):
    utility.logger.setup_logger_lambda()

    parser = get_parser()
    args = utility.lambda_helper.parse_events_as_args(parser, event)
    main(args)

    return {
        "statusCode": 200,
        "body": '{}',
    }
