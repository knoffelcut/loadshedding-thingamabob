import argparse

def parse_events_as_args(parser: argparse.ArgumentParser, event: dict):
    events = [s for k, v in event.items() for s in (f'--{k}', f'{v}')]

    args = parser.parse_args(events)

    return args
