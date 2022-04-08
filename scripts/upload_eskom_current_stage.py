
import argparse
import datetime

import scraping.scraping
import loadshedding_coct_stage_query.query_and_upload

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

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    loadshedding_coct_stage_query.query_and_upload.query_and_upload(
        **vars(args),
        suffix='current-stage',
        f_scrape=scraping.scraping.extract_eskom_loadshedding_stage,
        f_datapack=lambda x: x
        )

def lambda_handler(event, context):
    #TODO Move to a module (the function to interpret the event object as arguments)
    parser = get_parser()

    events = [s for k, v in event.items() for s in (f'--{k}', f'{v}')]

    args = parser.parse_args(events)

    loadshedding_coct_stage_query.query_and_upload.query_and_upload(
        **vars(args),
        suffix='current-stage',
        f_scrape=scraping.scraping.extract_eskom_loadshedding_stage,
        f_datapack=lambda x: x
        )

    return {
        "statusCode": 200,
        "body": '{}',
    }
