#!/usr/bin/env python3
"""Test runner for ETL jobs with isolated environments."""

import argparse
import subprocess
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


def find_changed_jobs():
    """Find jobs that have changed compared to main branch."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "origin/master", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        changed_files = result.stdout.strip().split("\n")
        jobs = set()

        for file in changed_files:
            # Handle external jobs
            if file.startswith("jobs/etl_jobs/external/"):
                parts = file.split("/")
                if len(parts) >= 4:
                    job_path = "/".join(parts[:4])
                    jobs.add(job_path)
            # Handle http_tools
            elif file.startswith("jobs/etl_jobs/http_tools/"):
                jobs.add("jobs/etl_jobs/http_tools")

        return list(jobs)
    except subprocess.CalledProcessError:
        print("Warning: Could not determine changed files. Running all tests.")
        return find_all_testable_jobs()


def find_all_testable_jobs():
    """Find all jobs that have unit tests."""
    jobs = []

    # Find external jobs
    external_test_dir = Path("tests/unit/etl_jobs/external")
    if external_test_dir.exists():
        for job_test_dir in external_test_dir.iterdir():
            if job_test_dir.is_dir():
                job_path = f"jobs/etl_jobs/external/{job_test_dir.name}"
                if Path(job_path).exists():
                    jobs.append(job_path)

    # Check for http_tools
    http_tools_test_dir = Path("tests/unit/etl_jobs/http_tools")
    if http_tools_test_dir.exists() and Path("jobs/etl_jobs/http_tools").exists():
        jobs.append("jobs/etl_jobs/http_tools")

    # Check for other direct etl_jobs subdirectories with tests
    etl_test_dir = Path("tests/unit/etl_jobs")
    if etl_test_dir.exists():
        for test_dir in etl_test_dir.iterdir():
            if test_dir.is_dir() and test_dir.name not in ["external", "http_tools"]:
                job_path = f"jobs/etl_jobs/{test_dir.name}"
                if Path(job_path).exists():
                    jobs.append(job_path)

    return jobs


def run_job_test(job_path):
    """Run tests for a single job."""
    script_path = Path("tests/scripts/test_runner.sh")
    try:
        result = subprocess.run(
            [str(script_path), job_path], check=True, capture_output=True, text=True
        )
        return job_path, True, result.stdout
    except subprocess.CalledProcessError as e:
        return job_path, False, e.stderr


def main():
    parser = argparse.ArgumentParser(description="Run ETL job tests")
    parser.add_argument("--all", action="store_true", help="Run all testable jobs")
    parser.add_argument(
        "--changed", action="store_true", help="Run tests for changed jobs only"
    )
    parser.add_argument("--jobs", nargs="+", help="Specific jobs to test")
    parser.add_argument(
        "--parallel", type=int, default=4, help="Number of parallel jobs"
    )

    args = parser.parse_args()

    if args.all:
        jobs = find_all_testable_jobs()
    elif args.changed:
        jobs = find_changed_jobs()
    elif args.jobs:
        jobs = args.jobs
    else:
        parser.print_help()
        return 1

    if not jobs:
        print("No jobs to test.")
        return 0

    print(f"Running tests for {len(jobs)} jobs: {', '.join(jobs)}")

    failed_jobs = []
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        future_to_job = {executor.submit(run_job_test, job): job for job in jobs}

        for future in as_completed(future_to_job):
            job_path, success, output = future.result()
            if success:
                print(f"✅ {job_path}: PASSED")
            else:
                print(f"❌ {job_path}: FAILED")
                print(output)
                failed_jobs.append(job_path)

    if failed_jobs:
        print(f"\n❌ {len(failed_jobs)} jobs failed: {', '.join(failed_jobs)}")
        return 1
    else:
        print(f"\n✅ All {len(jobs)} jobs passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
