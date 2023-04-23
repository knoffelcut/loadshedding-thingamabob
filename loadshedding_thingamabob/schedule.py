import csv
import itertools
import bisect
import typing
import datetime
import zoneinfo


class Schedule(object):
    def __init__(self, schedule: typing.Iterable, timezone: str):
        self.schedule = []
        self.datetimes = []
        self.stages = []
        self.set_schedule(schedule, timezone)

        self.check_schedule()

    def __str__(self):
        time_ranges = \
            [('epoch', self.datetimes[0].isoformat())] + \
            [
                (alpha.isoformat(),
                 omega.isoformat())
                for alpha, omega in zip(self.datetimes, self.datetimes[1:])
            ] + \
            [(self.datetimes[-1].isoformat(), 'ragnarok')]

        stages = [0, ] + self.stages
        assert len(time_ranges) == len(stages)

        string = '\n'.join(f'[{alpha:32} - {omega:32}): {stage:2}' for (
            alpha, omega), stage in zip(time_ranges, stages))

        return string

    def set_schedule(self, schedule, timezone):
        schedule = [
            (
                datetime.datetime.fromtimestamp(int(line[0])).replace(tzinfo=zoneinfo.ZoneInfo(timezone)),
                int(line[1])
            )
            for line in schedule
        ]

        # Support duplicate entries given they are sequential
        schedule = [key for key, _ in itertools.groupby(schedule)]
        assert schedule

        self.schedule = schedule

        self.datetimes = [line[0] for line in self.schedule]
        self.stages = [line[1] for line in self.schedule]

    def check_schedule(self):
        for line in self.schedule:
            assert len(line) == 2, line

        assert len(self.datetimes) == len(set(self.datetimes)), "Duplicate datetimes in schedule"
        for ts0, ts1 in zip(self.datetimes, sorted(self.datetimes)):
            assert ts0 == ts1, "Datetimes in schedule not in sorted order"

        for stage in self.stages:
            assert 0 <= stage <= 8

    @staticmethod
    def from_file(path: str):
        with open(path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            schedule = []
            for row in csv_reader:
                schedule.append(row)

        return Schedule(schedule)

    def to_csv(self):
        assert len(self.datetimes) == len(self.stages)
        return '\n'.join([f'{t.timestamp()}, {s}' for t, s in zip(self.datetimes, self.stages)])

    @classmethod
    def from_string(cls, data: str, timezone):
        schedule = [row for row in csv.reader(
            data.split('\n'), delimiter=',') if row]

        return cls(schedule, timezone)

    def stage(self, datetime_query: datetime.datetime):
        assert isinstance(datetime_query, datetime.datetime)
        assert datetime_query.tzinfo is not None
        assert datetime_query.tzinfo.utcoffset(datetime_query) is not None

        i = bisect.bisect_right(self.datetimes, datetime_query)
        i = i - 1  # Such that -1 = not in list, otherwise correspond to stage index

        if i < 0:
            return 0

        return self.stages[i]


if __name__ == '__main__':
    schedule = Schedule.from_file('schedules/test0.csv')

    # TODO As unittest
    print(schedule.stage('2021-01-01 09:15:33'))  # 0
    print(schedule.stage('2022-03-21 09:15:33'))  # 0
    print(schedule.stage('2022-03-21 16:00'))  # 4
    print(schedule.stage('2022-03-21 16:05'))  # 4
    print(schedule.stage('2022-03-21 19:59:59'))  # 4
    print(schedule.stage('2022-03-21 20:00'))  # 2
    print(schedule.stage('2022-03-21 22:05'))  # 0
    print(schedule.stage('2022-03-22 22:05'))  # 3

    print()
    print(schedule)
