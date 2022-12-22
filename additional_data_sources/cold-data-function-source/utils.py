import os
from datetime import datetime
from google.cloud import storage, bigquery

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")
RAW_DATASET = os.environ.get("RAW_DATASET")
DATA_BUCKET = os.environ.get("DATA_BUCKET")
BIGQUERY_IMPORT_BUCKET_FOLDER = os.environ.get("BIGQUERY_IMPORT_BUCKET_FOLDER")


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

    # TODO : support othe file formats.
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

        uri = "gs://{}/{}/{}.{}".format(bucket_name, folder_name, file_name, file_type)

        table_id = "{}.{}.{}".format(
            gcp_project, destination_dataset, destination_table
        )

        load_job = bigquery_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

        destination_table = bigquery_client.get_table(table_id)

        print("Loaded {} rows.".format(destination_table.num_rows))
