import argparse


def parse_events_as_args(parser: argparse.ArgumentParser, event: dict):
    # Other event['region'] may be interpreted as 'region_loadshedding'
    parser.allow_abbrev = False

    events = [s for k, v in event.items() for s in (f'--{k}', f'{v}')]

    args, _ = parser.parse_known_args(events)

    return args
