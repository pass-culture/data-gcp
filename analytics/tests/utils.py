from google.cloud import bigquery

from analytics.tests.config import GCP_PROJECT, GCP_REGION, BIGQUERY_SCHEMAS


def drop_dataset(client, dataset):
    dataset_id = f"{GCP_PROJECT}.{dataset}"
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def create_dataset(client, dataset):
    dataset_id = f"{GCP_PROJECT}.{dataset}"
    bq_dataset = bigquery.Dataset(dataset_id)
    bq_dataset.location = GCP_REGION
    client.create_dataset(bq_dataset, timeout=30)


def drop_table(client, dataset, table):
    table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    client.delete_table(table_id, not_found_ok=True)


def create_table(client, dataset, table, table_prefix=""):
    table_id = f"{GCP_PROJECT}.{dataset}.{table_prefix}{table}"
    schema = [
        bigquery.SchemaField(col_name, col_type)
        for col_name, col_type in BIGQUERY_SCHEMAS[table].items()
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)


def insert_rows(client, dataset, table, rows, table_prefix=""):
    # table_id = f"{GCP_PROJECT}.{dataset}.{table}"
    # client.insert_rows_json(table_id, rows)  # does not work... (something to do with the async creation operation)

    job_config = bigquery.QueryJobConfig()
    job_config.destination = f"{GCP_PROJECT}.{dataset}.{table_prefix}{table}"
    job_config.write_disposition = "WRITE_APPEND"
    for row in rows:
        fields = ", ".join(
            [
                f"CAST('{val}' AS {BIGQUERY_SCHEMAS[table][col]}) AS {col}"
                for col, val in row.items()
                if val is not None
            ]
        )
        query = f"SELECT {fields};"
        query_job = client.query(query=query, job_config=job_config)
        query_job.result()


def create_data(client, dataset, data, table_prefix):
    for table_name, table_rows in data.items():
        create_table(
            client=client, dataset=dataset, table=table_name, table_prefix=table_prefix
        )
        insert_rows(
            client=client,
            dataset=dataset,
            table=table_name,
            rows=table_rows,
            table_prefix=table_prefix,
        )


def run_query(client, query):
    query_job = client.query(query=query)
    query_job.result()


def retrieve_data(client, dataset, table, table_prefix=""):
    table_id = f"{GCP_PROJECT}.{dataset}.{table_prefix}{table}"
    rows_iter = client.list_rows(table_id)
    return [dict(row.items()) for row in rows_iter]


def get_table_columns(client, dataset, table, table_prefix=""):
    table_id = f"{GCP_PROJECT}.{dataset}.{table_prefix}{table}"
    table = client.get_table(table_id)
    return [field.name for field in table.schema]
