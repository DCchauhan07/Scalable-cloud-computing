#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

# Defining the function of Mapper and Reducer phase
class MRLatestFatalAppError(MRJob):

    def mapper_extract_fatal_app_error(self, _, line):
        columns = line.split()
        if len(columns) >= 10:
            timestamp_str = columns[1]
            date_str = columns[2]
            date_time_str = columns[4]
            level = columns[8]
            message_content = ' '.join(columns[9:])
            if level == "FATAL" and "message prefix on control stream" in message_content:
                yield None, (date_time_str, timestamp_str)

    def reducer_find_latest(self, _, date_time_pairs):
        latest_date_time = max(date_time_pairs, key=lambda x: int(x[1]))
        yield "Latest Fatal App Error", latest_date_time

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_fatal_app_error,
                   reducer=self.reducer_find_latest)
        ]

if __name__ == '__main__':
    start_time = time.time()
    MRLatestFatalAppError.run()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time} seconds")
