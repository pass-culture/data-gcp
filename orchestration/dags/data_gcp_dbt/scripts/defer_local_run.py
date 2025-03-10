import argparse
import os
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage


def find_dotenv():
    # Python implementation of find_dotenv()
    current_dir = Path.cwd()
    while current_dir != Path("/"):
        if (current_dir / ".env.dbt").is_file():
            return current_dir / ".env.dbt"
        current_dir = current_dir.parent
    return None


def load_environment_variables():
    dotenv_path = find_dotenv()

    if dotenv_path:
        load_dotenv(dotenv_path=dotenv_path)
        print(f"Loaded environment variables from {dotenv_path}")
    else:
        print("Error: .env.dbt file not found.")
        exit(1)


# Function to find dbt_project.yml
def find_dbt_project_yml(directory):
    directory = Path(directory)
    while directory != Path("/"):
        if (directory / "dbt_project.yml").is_file():
            return directory
        directory = directory.parent
    return None


# Function to pull artifacts
def pull_artifacts(environment):
    client = storage.Client()
    if environment == "dev":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_DEV', '')}/data/target"
    elif environment == "stg":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_STG', '')}/data/target"
    elif environment == "prod":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_PROD', '')}/data/target"
    else:
        print(f"Unknown environment: {environment}. Unable to pull artifacts.")
        return 1

    dbt_project_dir = "."  # to check

    target_dir = dbt_project_dir / "env-run-artifacts" / environment
    target_dir.mkdir(parents=True, exist_ok=True)

    bucket_name = bucket_path.replace("gs://", "").split("/")[0]
    blob_prefix = "/".join(bucket_path.replace("gs://", "").split("/")[1:])

    bucket = client.bucket(bucket_name)

    manifest_blob = bucket.blob(f"{blob_prefix}/manifest.json")
    manifest_blob.download_to_filename(str(target_dir / "manifest.json"))
    if not (target_dir / "manifest.json").exists():
        print(f"Failed to pull manifest.json from {bucket_path}.")
        return 1

    run_results_blob = bucket.blob(f"{blob_prefix}/run_results.json")
    run_results_blob.download_to_filename(str(target_dir / "run_results.json"))
    if not (target_dir / "run_results.json").exists():
        print(f"Failed to pull run_results.json from {bucket_path}.")
        return 1

    print(
        f"Artifacts (manifest.json and run_results.json) pulled successfully from {environment}."
    )
    return 0


def dbt_sync_artifacts(environment):
    # Python implementation of dbt_sync_artifacts()
    client = storage.Client()
    if environment == "dev":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_DEV', '')}/data/target"
    elif environment == "stg":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_STG', '')}/data/target"
    elif environment == "prod":
        bucket_path = f"gs://{os.environ.get('COMPOSER_BUCKET_PROD', '')}/data/target"
    else:
        print(f"Unknown environment: {environment}. Unable to pull artifacts.")
        return 1
    target_dir = Path("target")
    target_dir.mkdir(exist_ok=True)
    artifacts_dir = target_dir / f"{environment}-run-artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    bucket = client.bucket(bucket_path.replace("gs://", "").split("/")[0])
    print(bucket)
    manifest_blob = bucket.blob(
        "/".join(bucket_path.replace("gs://", "").split("/")[1:]) + "/manifest.json"
    )
    manifest_blob.download_to_filename(str(artifacts_dir / "manifest.json"))
    run_results_blob = bucket.blob(
        "/".join(bucket_path.replace("gs://", "").split("/")[1:]) + "/run_results.json"
    )
    run_results_blob.download_to_filename(str(artifacts_dir / "run_results.json"))


def dbt_hook(args, remaining_args):
    """
    Python implementation of the dbt_hook() function.
    """

    # Set default values
    defer_local_run_to = args.defer_to or "dev"
    refresh_state = args.refresh_state or False
    sync_artifacts = args.sync_artifacts
    target_env = args.target
    sync_environments = ["dev", "stg", "prod"]

    # Set APPLICATIVE_EXTERNAL_CONNECTION_ID
    if defer_local_run_to == "none" and target_env:
        if target_env == "dev":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_DEV", ""
            )
        elif target_env == "stg":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_STG", ""
            )
        elif target_env == "prod":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_PROD", ""
            )
        else:
            print(f"Unknown target environment: {target_env}")
            return 1
    elif defer_local_run_to != "none":
        if defer_local_run_to == "dev":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_DEV", ""
            )
        elif defer_local_run_to == "stg":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_STG", ""
            )
        elif defer_local_run_to == "prod":
            os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"] = os.environ.get(
                "APPLICATIVE_EXTERNAL_CONNECTION_ID_PROD", ""
            )
        else:
            print(f"Unknown DEFER_LOCAL_RUN_TO environment: {defer_local_run_to}")
            return 1

    echo_env = target_env if defer_local_run_to == "none" else defer_local_run_to
    print(
        f"Set APPLICATIVE_EXTERNAL_CONNECTION_ID to {os.environ.get('APPLICATIVE_EXTERNAL_CONNECTION_ID', '')} for environment {echo_env}"
    )

    # Check if --sync-artifacts is used alone
    if sync_artifacts and remaining_args:
        print(
            "Error: The --sync-artifacts flag must be used alone. No other arguments are allowed."
        )
        print(
            "Hint: If you want to refresh artifacts and run a dbt command, use --refresh-state instead."
        )
        return 1

    # Set project directory
    project_dir = Path.cwd()

    # Check if dbt_project.yml exists
    dbt_project_dir = find_dbt_project_yml(project_dir)
    if not dbt_project_dir:
        print(
            "Error: dbt_project.yml not found. This script must be run from within a dbt project folder."
        )
        return 1
    dbt_profiles_dir = str(dbt_project_dir)
    os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir

    # If --sync-artifacts is provided
    if sync_artifacts:
        print(f"Syncing artifacts from environments: {', '.join(sync_environments)}")
        for env in sync_environments:
            if pull_artifacts(env) != 0:
                return 1
        return 0

    # If --refresh-state is provided and no dbt command
    if refresh_state and not remaining_args:
        print("Refreshing state without dbt command: pulling from dev, stg, and prod.")
        for env in ["dev", "stg", "prod"]:
            if pull_artifacts(env) != 0:
                return 1
        return 0

    # If --refresh-state and --defer-to are provided
    if refresh_state and defer_local_run_to != "none":
        print(
            f"Refreshing state and pulling artifacts (manifest.json and run_results.json) from {defer_local_run_to}."
        )
        if pull_artifacts(defer_local_run_to) != 0:
            return 1
        return 0

    # Dbt hook logic for dbt run/test
    artifacts_dir = dbt_project_dir / "env-run-artifacts" / defer_local_run_to
    defer_flags = []

    if defer_local_run_to != "none":
        manifest_path = artifacts_dir / "manifest.json"
        results_path = artifacts_dir / "run_results.json"

        need_pull = (
            not manifest_path.exists() or not results_path.exists() or refresh_state
        )
        if need_pull:
            print("Pulling or refreshing artifacts...")
            if pull_artifacts(defer_local_run_to) != 0:
                return 1
        else:
            print(
                "Using existing artifacts. Consider using --refresh-state to ensure freshness."
            )
        defer_flags = ["--defer", "--state", str(artifacts_dir), "--favor-state"]
    else:
        print("Skipping artifact pulling as --defer-to=none.")

    if args.select:
        select_flag = ["-s", args.select]

    combined_args = select_flag + remaining_args + defer_flags

    print(f"Running dbt with arguments: {' '.join(combined_args)}")
    result = subprocess.run(
        ["dbt"] + [args.dbt_command] + combined_args, capture_output=True, text=True
    )
    # print(result.stdout)
    # if result.stderr:
    #     print(result.stderr)
    # return result.returncode

    if result.returncode == 0:
        print("dbt command succeeded!")
        print(result.stdout)
    else:
        print("dbt command failed!")
        print(result.stderr)


def main():
    load_environment_variables()
    parser = argparse.ArgumentParser(description="Custom dbt wrapper.")
    parser.add_argument("dbt_command", nargs="?")
    parser.add_argument("-s", "--select", nargs="?")
    parser.add_argument(
        "--defer-to", choices=["dev", "stg", "prod", "none"], default="dev"
    )
    parser.add_argument("--refresh-state", default=False)
    parser.add_argument("--sync-artifacts", nargs="?")  # Optional argument
    parser.add_argument(
        "-t", "--target", choices=["local", "dev", "stg", "prod"], default="local"
    )
    args, remaining_args = parser.parse_known_args()
    print(f"args={args}")
    print(f"remaining_args={remaining_args}")

    if args.sync_artifacts:
        if args.sync_artifacts != "all":
            dbt_sync_artifacts(args.sync_artifacts)
        else:
            for env in ["dev", "stg", "prod"]:
                dbt_sync_artifacts(env)
        return

    if args.dbt_command in ["run", "test"]:
        dbt_hook(args, remaining_args)
    else:
        print("call subprocess dbt")
        subprocess.run(["dbt"] + [args.dbt_command] + remaining_args)


if __name__ == "__main__":
    main()
