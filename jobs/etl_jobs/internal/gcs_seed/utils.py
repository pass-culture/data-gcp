import os

from google.cloud import bigquery

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
SEED_DATASET = f"seed_{ENVIRONMENT_SHORT_NAME}"
DE_BIGQUERY_DATA_IMPORT_BUCKET_NAME = (
    f"de-bigquery-data-import-{ENVIRONMENT_SHORT_NAME}"
)
BIGQUERY_IMPORT_BUCKET_FOLDER = "bigquery_imports/seed"


def GCS_to_bigquery(
    gcp_project,
    bigquery_client,
    bucket_name,
    folder_name,
    file_name,
    file_type,
    destination_dataset,
    destination_table,
    schema,
):
    if file_type == "csv":
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(column, _type) for column, _type in schema.items()
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition="WRITE_TRUNCATE",
            encoding="UTF-8",
        )
    elif file_type == "parquet":
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
            encoding="UTF-8",
        )
    elif file_type == "avro":
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.AVRO,
            write_disposition="WRITE_TRUNCATE",
            encoding="UTF-8",
        )
    else:
        raise Exception("Format not found")

    uri = "gs://{}/{}/{}.{}".format(bucket_name, folder_name, file_name, file_type)

    table_id = "{}.{}.{}".format(gcp_project, destination_dataset, destination_table)

    load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)

    load_job.result()

    destination_table = bigquery_client.get_table(table_id)

    print("Loaded {} rows.".format(destination_table.num_rows))
