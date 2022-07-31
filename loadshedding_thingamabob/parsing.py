import os
import time
import datetime
import json

import loadshedding_thingamabob.schedule

class ParseError(ValueError):
    pass

def extract_national_loadshedding_stage(response_text: str):
    """Extracts the current National loadshedding stage

    Args:
        response_text (str): National API response

    Returns:
        int: Current national loadshedding stage
    """
    try:
        stage = int(response_text)
        stage = stage - 1
        stage = max(0, stage)
        return stage
    except Exception as e:
        raise ParseError from e

def convert_national_plaintext_to_schedule(timestamp_db: int, plaintext: str):
    if isinstance(plaintext, str):
        plaintext = extract_national_loadshedding_stage(plaintext)
    assert isinstance(plaintext, int)

    schedule = [
        (timestamp_db, plaintext),
    ]
    schedule = loadshedding_thingamabob.schedule.Schedule(schedule)

    return schedule

def convert_coct_plaintext_to_schedule(timestamp_db: int, plaintext: str):
    assert isinstance(plaintext, str)

    data = json.loads(plaintext)
    data = data[0]

    # Parsing of lastUpdated ain't perfect here (note https://stackoverflow.com/a/30696682)
    #   since we throw away the timezone information
    #   but good enough for now, since we only need to parse a single and consistent format
    # last_updated is not used right now, but it can be used when last_updated < start_time
    #   In that case we actually need to get the previous schedule and get the current stage
    #   for the epoch:start_start stage
    # assert data['lastUpdated'][-1].lower() == 'z'
    # last_updated = datetime.datetime.fromisoformat(data['lastUpdated'][:-1])

    current_stage = int(data['currentStage'])
    next_stage = int(data['nextStage'])

    system_tz = os.environ['TZ'] if 'TZ' in os.environ else None
    os.environ['TZ'] = 'Africa/Johannesburg'
    time.tzset()
    try:
        start_time = datetime.datetime.fromisoformat(data['startTime']).timestamp()
        next_stage_start_time = datetime.datetime.fromisoformat(data['nextStageStartTime']).timestamp()
    finally:
        if system_tz:
            os.environ['TZ'] = system_tz
        else:
            del os.environ['TZ']
        time.tzset()

    if next_stage_start_time < start_time:
        # This is a valid response from the CoCT API
        # TODO log.warning or similar upon condition
        schedule = [
            (start_time, current_stage),
        ]
    else:
        schedule = [
            (start_time, current_stage),
            (next_stage_start_time, next_stage),
        ]

    schedule = loadshedding_thingamabob.schedule.Schedule(schedule)

    return schedule

# TODO Move to external development script
def development_coct():
    import pickle
    import datetime

    try:
        # raise FileNotFoundError
        with open('cache_coct_plaintext.pkl', 'rb') as f:
            items  = pickle.load(f)
    except FileNotFoundError as e:
        import boto3
        import database.dynamodb

        region_loadshedding = 'coct'
        suffix = 'plaintext'
        table_name = 'loadshedding'

        dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
        table = dynamodb.Table(table_name)

        partition_key = f"{region_loadshedding}-{suffix}"
        items = database.dynamodb.get_recent_scraped_data(table, partition_key, 128, False)

        with open('cache_coct_plaintext.pkl', 'wb') as f:
            pickle.dump(items, f, pickle.HIGHEST_PROTOCOL)

    for i, item in enumerate(items[::-1]):
        timestamp = int(item['timestamp'])
        data = item['data']

        schedule = convert_coct_plaintext_to_schedule(timestamp, data)

        print(f'{i:02d} {timestamp} {datetime.datetime.fromtimestamp(timestamp)}')
        print(data)
        print(schedule)
        print()

# TODO Move to external development script
def development_national():
    import pickle
    import datetime

    try:
        # raise FileNotFoundError
        with open('cache_national_plaintext.pkl', 'rb') as f:
            items  = pickle.load(f)
    except FileNotFoundError as e:
        import boto3
        import database.dynamodb

        region_loadshedding = 'national'
        suffix = 'plaintext'
        table_name = 'loadshedding'

        dynamodb = boto3.resource('dynamodb', region_name='af-south-1')
        table = dynamodb.Table(table_name)

        partition_key = f"{region_loadshedding}-{suffix}"
        items = database.dynamodb.get_recent_scraped_data(table, partition_key, 128, False)

        with open('cache_national_plaintext.pkl', 'wb') as f:
            pickle.dump(items, f, pickle.HIGHEST_PROTOCOL)

    for i, item in enumerate(items[::-1]):
        timestamp = int(item['timestamp'])
        data = item['data']


        print(f'{i:02d} {timestamp} {datetime.datetime.fromtimestamp(timestamp)}')
        print(f'data: {data}')
        try:
            schedule = convert_national_plaintext_to_schedule(timestamp, data)
            print(schedule)
        except Exception as e:
            print(e)
        print()


if __name__ == '__main__':
    development_national()
