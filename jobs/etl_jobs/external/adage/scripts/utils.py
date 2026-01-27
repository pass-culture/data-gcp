import os
from datetime import datetime

from google.cloud import bigquery

GCP_PROJECT = os.environ.get("PROJECT_NAME")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_TMP_DATASET = f"tmp_{ENV_SHORT_NAME}"
BUCKET_NAME = f"data-bucket-{ENV_SHORT_NAME}"


ADAGE_INVOLVED_STUDENTS_DTYPE = {
    "metric_name": str,
    "metric_id": str,
    "educational_year_adage_id": int,
    "metric_key": str,
    "level": str,
    "involved_students": str,
    "institutions": str,
    "total_involved_students": str,
    "total_institutions": str,
}

BQ_ADAGE_DTYPE = {
    "id": "STRING",
    "siret": "STRING",
    "venueId": "STRING",
    "regionId": "STRING",
    "academieId": "STRING",
    "statutId": "STRING",
    "labelId": "STRING",
    "typeId": "STRING",
    "communeId": "STRING",
    "libelle": "STRING",
    "adresse": "STRING",
    "siteWeb": "STRING",
    "latitude": "STRING",
    "longitude": "STRING",
    "actif": "STRING",
    "synchroPass": "STRING",
    "dateModification": "STRING",
    "statutLibelle": "STRING",
    "labelLibelle": "STRING",
    "typeIcone": "STRING",
    "typeLibelle": "STRING",
    "communeLibelle": "STRING",
    "communeDepartement": "STRING",
    "academieLibelle": "STRING",
    "regionLibelle": "STRING",
    "domaines": "STRING",
    "update_date": "STRING",
}


class RequestReturnedNoneError(Exception):
    """Custom exception for when a request returns None."""

    pass


def save_to_raw_bq(df, table_name):
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["execution_date"] = _now
    bigquery_client = bigquery.Client()
    table_id = f"{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
    )
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
