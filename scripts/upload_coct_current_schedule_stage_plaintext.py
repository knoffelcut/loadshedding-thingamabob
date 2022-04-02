import datetime

import scraping.scraping
import loadshedding_coct_stage_query.query_and_upload

if __name__ == '__main__':
    import argparse

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
    args = parser.parse_args()

    loadshedding_coct_stage_query.query_and_upload.query_and_upload(
        **vars(args),
        suffix='plaintext',
        f_scrape=scraping.scraping.extract_coct_loadshedding_text,
        f_datapack=lambda data: '\n'.join(data)
        )
