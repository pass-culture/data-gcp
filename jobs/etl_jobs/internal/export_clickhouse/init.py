import typer
from core.utils import (
    clickhouse_client,
    access_secret_data,
    PROJECT_NAME,
    ENV_SHORT_NAME,
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
            sql_query = f""" CREATE IF NOT EXISTS DATABASE {db}  ON cluster default """
            print(f"Will Execute: {sql_query}")
            clickhouse_client.command(sql_query)
        except Exception as e:
            print(f"Something went wrong: {e}")
    try:
        named_collection = f"""
            CREATE NAMED COLLECTION gcs_credentials on cluster default AS 
                access_key_id = '{access_key_id}',
                secret_access_key = '{secret_access_key}'
        """
        clickhouse_client.command(named_collection)
    except Exception as e:
        print(f"Something went wrong: {e}")


def run():
    init()


if __name__ == "__main__":
    typer.run(run)
