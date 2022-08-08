import csv
import datetime
import pathlib
import warnings

import loadshedding_thingamabob.schedule


def main(path: str, tag: str):
    path = pathlib.Path(path)

    if path.is_file():
        paths = [path, ]
    elif path.is_dir():
        paths = list(path.rglob(f'*{tag}.csv'))
        if len(paths) == 0:
            warnings.warn(
                f"'{path}' does not contain any files ending in '*{tag}.csv'")
    else:
        raise FileNotFoundError(f"No file or directory named '{path}' exists")

    for path in paths:
        file_destination = (
            path.parent / path.stem[:-len(tag)]).with_suffix('.csv')
        print(f"Converting '{path}' to '{file_destination}'")

        with open(path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            lines = []
            for row in csv_reader:
                lines.append(row)

        isoformats = [line[0] for line in lines]
        timestamps = [int(datetime.datetime.fromisoformat(
            isoformat).timestamp()) for isoformat in isoformats]

        lines = [(timestamp, stage)
                 for timestamp, (_, stage) in zip(timestamps, lines)]

        try:
            schedule = loadshedding_thingamabob.schedule.Schedule(lines)
        except AssertionError as e:
            raise AssertionError(
                f'Error when converting schedule "{path}"') from e
        print(f'Schedule:\n{schedule}')

        with open(file_destination, 'w') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            csv_writer.writerows(lines)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Convert a datetime .csv schedule to a timestamp .csv schedule')
    parser.add_argument('--path', type=str, default='schedules',
                        help='Path to ".csv" file. If path points to a directory, all files matching "*_<tag>.csv" will be converted'
                        )
    parser.add_argument('--tag', type=str, default='_isoformat',
                        help='Tag of all isoformat date files'
                        )
    args = parser.parse_args()

    main(**vars(args))
