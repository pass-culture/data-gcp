import sys

from google.cloud import bigquery

from bigquery.config import (
    GCP_REGION,
    BIGQUERY_POC_DATASET,
    CLOUDSQL_DATABASE,
    OFFER_TABLE_NAME,
    OFFER_COLUMNS,
    OFFER_ID,
)
from bigquery.utils import run_query
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def retrieve_last_check(bq_client, table_name):
    # define query
    query = f"SELECT MAX(lastUpdate) AS last_check FROM {BIGQUERY_POC_DATASET}.{table_name};"

    # run query
    query_job = bq_client.query(query=query)
    results = query_job.result()

    return list(results)[0]["last_check"]


def define_upsert_query(last_check, table_name, columns, id_column):
    formatted_set = ", ".join(
        [f"{col} = csql_table.{col}" for col in columns if col != id_column]
    )
    formatted_insert = ", ".join(columns)
    query = f"""
        MERGE {BIGQUERY_POC_DATASET}.{table_name} bq_table
        USING (SELECT * FROM EXTERNAL_QUERY(
            '{GCP_REGION}.{CLOUDSQL_DATABASE}',
            "SELECT * FROM {table_name} WHERE lastUpdate > '{str(last_check)}'"
            )
        ) csql_table
        ON bq_table.{id_column} = csql_table.{id_column}
        WHEN MATCHED THEN
            UPDATE SET {formatted_set}
        WHEN NOT MATCHED THEN
            INSERT ({formatted_insert}) VALUES({formatted_insert});
    """

    return query


def define_delete_query(table_name, id_column):
    query = f"""
    DELETE {BIGQUERY_POC_DATASET}.{table_name} bq_table
    WHERE bq_table.{id_column} NOT IN (
        SELECT {id_column} FROM EXTERNAL_QUERY(
            "{GCP_REGION}.{CLOUDSQL_DATABASE}",
            "SELECT * FROM {table_name};"
        )
    );
    """

    return query


def main():
    client = bigquery.Client()

    # Retrieve timestamp of last data in offer
    last_check = retrieve_last_check(client, table_name=OFFER_TABLE_NAME)

    # Generate queries for offer table
    upsert_query = define_upsert_query(
        last_check=last_check,
        table_name=OFFER_TABLE_NAME,
        columns=OFFER_COLUMNS,
        id_column=OFFER_ID,
    )
    delete_query = define_delete_query(table_name=OFFER_TABLE_NAME, id_column=OFFER_ID)

    # Run queries
    run_query(bq_client=client, query=upsert_query)
    run_query(bq_client=client, query=delete_query)


if __name__ == "__main__":
    set_env_vars()
    main()
