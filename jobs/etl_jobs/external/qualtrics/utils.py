import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from google.cloud import bigquery

from datetime import datetime

PROJECT_NAME = os.environ.get("PROJECT_NAME")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BIGQUERY_RAW_DATASET = f"raw_{ENVIRONMENT_SHORT_NAME}"


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_to_raw_bq(df, table_name, schema):
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["execution_date"] = _now
    bigquery_client = bigquery.Client()
    table_id = f"{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField(column, _type) for column, _type in schema.items()
        ],
    )
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


API_TOKEN = access_secret_data(
    PROJECT_NAME, f"qualtrics_token_{ENVIRONMENT_SHORT_NAME}"
)
DATA_CENTER = access_secret_data(
    PROJECT_NAME, f"qualtrics_data_center_{ENVIRONMENT_SHORT_NAME}"
)
DIRECTORY_ID = access_secret_data(
    PROJECT_NAME, f"qualtrics_directory_id_{ENVIRONMENT_SHORT_NAME}"
)

OPT_OUT_EXPORT_COLUMNS = {
    "contactId": "contact_id",
    "firstName": "first_name",
    "lastName": "last_name",
    "email": "email",
    "phone": "phone",
    "language": "language",
    "extRef": "ext_ref",
    "directoryUnsubscribed": "directory_unsubscribed",
    "directoryUnsubscribeDate": "directory_unsubscribe_date",
}

IR_JEUNES_TABLE_SCHEMA = {
    "StartDate": "STRING",
    "EndDate": "STRING",
    "ResponseId": "STRING",
    "ExternalReference": "STRING",
    "theoretical_amount_spent": "STRING",
    "user_activity": "STRING",
    "user_civility": "STRING",
    "Q3_Topics": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "question_str": "STRING",
    "question_id": "STRING",
    "user_type": "STRING",
}

IR_PRO_TABLE_SCHEMA = {
    "StartDate": "STRING",
    "EndDate": "STRING",
    "ResponseId": "STRING",
    "ExternalReference": "STRING",
    "anciennete_jours": "STRING",
    "non_cancelled_bookings": "STRING",
    "offers_created": "STRING",
    "Q1_Topics": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "question_str": "STRING",
    "question_id": "STRING",
    "user_type": "STRING",
}
