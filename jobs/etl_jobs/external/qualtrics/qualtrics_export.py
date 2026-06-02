import os

import pandas as pd
from google.cloud import bigquery
from loguru import logger

from qualtrics_client import QualtricsClient


def export_beneficiary_to_qualtrics(
    ds: str,
    dataset_name: str,
    table_name: str,
    directory_id: str,
    mailing_list_id: str,
    client: QualtricsClient,
) -> list[dict]:
    return _export_to_qualtrics(
        ds=ds,
        dataset_name=dataset_name,
        table_name=table_name,
        directory_id=directory_id,
        mailing_list_id=mailing_list_id,
        client=client,
        audit_table="qualtrics_exported_beneficiary_account",
        email_column="email",
        ext_ref_column="user_id",
        embedded_data_columns={
            "age": "user_age",
            "civility": "user_civility",
            "status": "deposit_type",
            "seniority_days": "user_seniority",
            "rural_city_type": "user_rural_city_type",
            "region": "user_region_name",
            "diversity_score": "total_diversity_score",
            "qpv_code": "qpv_code",
        },
        log_label="Beneficiary",
    )


def export_venue_to_qualtrics(
    ds: str,
    dataset_name: str,
    table_name: str,
    directory_id: str,
    mailing_list_id: str,
    client: QualtricsClient,
) -> list[dict]:
    return _export_to_qualtrics(
        ds=ds,
        dataset_name=dataset_name,
        table_name=table_name,
        directory_id=directory_id,
        mailing_list_id=mailing_list_id,
        client=client,
        audit_table="qualtrics_exported_venue_account",
        email_column="venue_booking_email",
        ext_ref_column="venue_id",
        embedded_data_columns={
            "venue_name": "venue_name",
            "venue_type_label": "venue_type_label",
            "seniority_days": "venue_seniority_days",
            "region": "venue_region_name",
            "department": "venue_department_code",
            "rural_city_type": "venue_rural_city_type",
            "total_real_revenue": "total_real_revenue",
            "total_non_cancelled_individual_bookings": (
                "total_non_cancelled_individual_bookings"
            ),
            "total_non_cancelled_collective_bookings": (
                "total_non_cancelled_collective_bookings"
            ),
            "is_active_current_year": "is_active_current_year",
            "is_individual_active_current_year": "is_individual_active_current_year",
            "is_collective_active_current_year": "is_collective_active_current_year",
        },
        log_label="Venue",
    )


def _export_to_qualtrics(
    ds: str,
    dataset_name: str,
    table_name: str,
    directory_id: str,
    mailing_list_id: str,
    client: QualtricsClient,
    audit_table: str,
    email_column: str,
    ext_ref_column: str,
    embedded_data_columns: dict[str, str],
    log_label: str,
) -> list[dict]:
    project = os.environ["PROJECT_NAME"]
    env = os.environ["ENV_SHORT_NAME"]
    raw_dataset = f"raw_{env}"

    bq = bigquery.Client(project=project)
    df = bq.query(
        f"SELECT * FROM `{project}.{dataset_name}.{table_name}`"
    ).to_dataframe()
    logger.info(f"{log_label} export: {len(df)} rows")

    _write_audit_table(bq, df, project, raw_dataset, audit_table, ds)

    contacts = _build_contacts(
        df,
        email_column=email_column,
        ext_ref_column=ext_ref_column,
        embedded_data_columns=embedded_data_columns,
    )
    logger.info(f"{log_label} contacts ready: {len(contacts)}")

    results = client.import_contacts(directory_id, mailing_list_id, contacts)
    logger.info(f"{log_label} import complete: {len(results)} chunk(s)")
    return results


def _build_contacts(
    df: pd.DataFrame,
    email_column: str,
    ext_ref_column: str,
    embedded_data_columns: dict[str, str],
) -> list[dict]:
    return [
        {
            "email": row[email_column],
            "extRef": str(row[ext_ref_column]),
            "embeddedData": {
                key: _stringify(row.get(source))
                for key, source in embedded_data_columns.items()
            },
        }
        for _, row in df.iterrows()
        if pd.notna(row.get(email_column))
    ]


def _stringify(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value)


def _write_audit_table(
    bq: bigquery.Client,
    df: pd.DataFrame,
    project: str,
    raw_dataset: str,
    table_name: str,
    ds: str,
) -> None:
    partition_date = ds[:7].replace("-", "") + "01"
    audit_df = df.copy()
    audit_df["calculation_month"] = pd.to_datetime(partition_date)
    table_id = f"{project}.{raw_dataset}.{table_name}${partition_date}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(field="calculation_month"),
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = bq.load_table_from_dataframe(audit_df, table_id, job_config=job_config)
    job.result()
    logger.info(f"Audit written to {table_id}: {len(audit_df)} rows")
