import typer
from core.utils import (
    CLICKHOUSE_CLIENT,
    ENV_SHORT_NAME,
    PROJECT_NAME,
    access_secret_data,
)

DATABASE = ["tmp", "intermediate", "analytics"]

access_key_id = access_secret_data(
    PROJECT_NAME, f"clickhouse-s3_access_id-{ENV_SHORT_NAME}"
)
secret_access_key = access_secret_data(
    PROJECT_NAME, f"clickhouse-s3_secret_key-{ENV_SHORT_NAME}"
)


def init() -> None:
    """Create default configuration for Clickhouse."""
    for db in DATABASE:
        try:
            sql_query = f""" CREATE DATABASE IF NOT EXISTS {db} ON cluster default """
            print(f"Will Execute: {sql_query}")
            CLICKHOUSE_CLIENT.command(sql_query)
        except Exception as e:
            print(f"Something went wrong: {e}")
    try:
        named_collection = f"""
            CREATE NAMED COLLECTION gcs_credentials on cluster default AS 
                access_key_id = '{access_key_id}',
                secret_access_key = '{secret_access_key}'
        """
        CLICKHOUSE_CLIENT.command(named_collection)
    except Exception as e:
        print(f"Something went wrong: {e}")


def run():
    init()


if __name__ == "__main__":
    typer.run(run)
