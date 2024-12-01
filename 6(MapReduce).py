#!/usr/bin/env python3

import datetime
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
import time

# Defining the function of Mapper and Reducer
class MRTorusReceiverErrors(MRJob):
    log_pattern = re.compile(r'- \d+ \d{4}\.\d{2}\.\d{2} .+ (\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2})\.\d+ .+ RAS .+ torus receiver z\+ input pipe error')

    def mapper_extract_errors(self, _, line):
        match = self.log_pattern.match(line)
        if match:
            timestamp_str = match.group(1)
            timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d-%H.%M.%S')
            day_of_week = timestamp.strftime('%A')
            yield day_of_week, float(timestamp.strftime('%S'))

    def reducer_average_errors(self, day, counts):
        total_counts = list(counts)
        yield day, sum(total_counts) / len(total_counts)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_errors,
                   reducer=self.reducer_average_errors),
        ]

if __name__ == '__main__':
    start_time = time.time()
    MRTorusReceiverErrors.run()
    
    # End the timer and print the total execution time
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total execution time: {total_time} seconds")
