#!/usr/bin/env python3
import ast
import sys
from pathlib import Path


def format_input(string):
    return string.replace('\\"', '"')


def main():
    # Extract values from JSON
    changed_etl_files = ast.literal_eval(format_input(sys.argv[1]))
    changed_ml_files = ast.literal_eval(format_input(sys.argv[2]))
    testable_jobs = ast.literal_eval(format_input(sys.argv[3]))

    # Function to check if a file is in the testable jobs list or any of its parent directories
    def is_testable(file_path, testable_jobs):
        file_path = Path(file_path)
        for job in testable_jobs:
            job_path = Path(job)
            if file_path == job_path or job_path in file_path.parents:
                return job
        return None

    # Compute the jobs to test
    jobs_to_test_set = set()
    for file in changed_etl_files + changed_ml_files:
        job = is_testable(file, testable_jobs)
        if job:
            jobs_to_test_set.add(job)

    # Convert set to sorted list
    jobs_to_test_list = sorted(jobs_to_test_set)

    # Output as JSON
    print(f"jobs={jobs_to_test_list}")


if __name__ == "__main__":
    main()
