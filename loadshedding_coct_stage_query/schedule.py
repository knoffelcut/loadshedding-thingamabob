import pathlib
import csv
import bisect
import typing
import datetime

class Schedule(object):
    def __init__(self, schedule: typing.Iterable):
        self.schedule = []
        self.timestamps = []
        self.stages = []
        self.set_schedule(schedule)

        self.check_schedule()

    def set_schedule(self, schedule):
        self.schedule = schedule
        self.timestamps = [int(line[0]) for line in self.schedule]
        self.stages = [int(line[1]) for line in self.schedule]

    def check_schedule(self):
        for line in self.schedule:
            assert len(line) == 2, line

        assert len(self.timestamps) == len(set(self.timestamps)), "Duplicate timestamps in schedule"
        for ts0, ts1 in zip(self.timestamps, sorted(self.timestamps)):
            assert ts0 == ts1, "Timestamps in schedule not in sorted order"

        for stage in self.stages:
            assert 0 <= stage <= 8

    @staticmethod
    def from_file(path:str):
        with open(path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            schedule = []
            for row in csv_reader:
                schedule.append(row)

        return Schedule(schedule)

    def stage(self, timestamp: int):
        try:
            int(timestamp)
        except ValueError as _:
            timestamp = int(datetime.datetime.fromisoformat(timestamp).timestamp())

        i = bisect.bisect_right(self.timestamps, timestamp)
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
