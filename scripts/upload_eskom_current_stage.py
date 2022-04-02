import datetime

import scraping.scraping
import loadshedding_coct_stage_query.query_and_upload

if __name__ == '__main__':
    import argparse

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
    args = parser.parse_args()

    loadshedding_coct_stage_query.query_and_upload.query_and_upload(
        **vars(args),
        suffix='current-stage',
        f_scrape=scraping.scraping.extract_eskom_loadshedding_stage,
        f_datapack=lambda x: x
        )
