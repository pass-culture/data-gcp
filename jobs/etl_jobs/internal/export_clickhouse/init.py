import logging

import typer

from core.utils import (
    ENV_SHORT_NAME,
    PROJECT_NAME,
    access_secret_data,
    get_clickhouse_client,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = typer.Typer()

DATABASE = ["tmp", "intermediate", "analytics"]

access_key_id = access_secret_data(
    PROJECT_NAME, f"clickhouse-s3_access_id-{ENV_SHORT_NAME}"
)
secret_access_key = access_secret_data(
    PROJECT_NAME, f"clickhouse-s3_secret_key-{ENV_SHORT_NAME}"
)


def init() -> None:
    """Create default configuration for Clickhouse."""
    client = get_clickhouse_client()
    for db in DATABASE:
        try:
            sql_query = f""" CREATE DATABASE IF NOT EXISTS {db} ON cluster default """
            logger.info(f"Will Execute: {sql_query}")
            client.command(sql_query)
        except Exception as e:
            logger.error(f"Failed to create database {db!r}: {e}")
            raise RuntimeError(f"Failed to create database {db!r}") from e
    try:
        named_collection = f"""
            CREATE NAMED COLLECTION gcs_credentials on cluster default AS
                access_key_id = '{access_key_id}',
                secret_access_key = '{secret_access_key}'
        """
        client.command(named_collection)
    except Exception as e:
        logger.error(f"Failed to create named collection: {e}")
        raise RuntimeError("Failed to create GCS named collection") from e


@app.command()
def run():
    init()


if __name__ == "__main__":
    app()
