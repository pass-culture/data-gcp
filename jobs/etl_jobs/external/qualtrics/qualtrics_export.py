import os

import pandas as pd
from google.cloud import bigquery

from qualtrics_client import QualtricsClient


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=os.environ["PROJECT_NAME"])


def export_beneficiary_to_qualtrics(
    ds: str,
    directory_id: str,
    mailing_list_id: str,
    client: QualtricsClient,
) -> dict:
    project = os.environ["PROJECT_NAME"]
    env = os.environ["ENV_SHORT_NAME"]
    applicative_connection_id = os.environ["APPLICATIVE_EXTERNAL_CONNECTION_ID"]
    export_dataset = f"export_{env}"
    raw_dataset = f"raw_{env}"

    sql = f"""
        WITH user_email AS (
            SELECT * FROM EXTERNAL_QUERY(
                '{applicative_connection_id}',
                'SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user'
            )
        ),
        export_data AS (
            SELECT
                t.*,
                date_trunc(date("{ds}"), month) as calculation_month,
                current_timestamp() as export_date
            FROM `{project}.{export_dataset}.qualtrics_beneficiary_account` t
        )
        SELECT
            e.*,
            ue.email
        FROM export_data e
        LEFT JOIN user_email ue ON ue.user_id = e.user_id
    """
    bq = _bq_client()
    df = bq.query(sql).to_dataframe()
    print(f"Beneficiary export: {len(df)} rows")

    _write_audit_table(
        bq, df, project, raw_dataset, "qualtrics_exported_beneficiary_account", ds
    )

    contacts = [
        {
            "email": row["email"],
            "extRef": str(row["user_id"]),
            "embeddedData": {
                "age": str(row.get("user_age", "")),
                "civility": str(row.get("user_civility", "")),
                "activity": str(row.get("user_activity", "")),
                "seniority_days": str(row.get("user_seniority", "")),
                "rural_city_type": str(row.get("user_rural_city_type", "")),
                "region": str(row.get("user_region_name", "")),
                "diversity_score": str(row.get("total_diversity_score", "")),
                "qpv_code": str(row.get("qpv_code", "")),
                "deposit_type": str(row.get("deposit_type", "")),
                "amount_spent": str(row.get("total_actual_amount_spent", "")),
                "non_cancelled_bookings": str(
                    row.get("total_non_cancelled_individual_bookings", "")
                ),
            },
        }
        for _, row in df.iterrows()
        if pd.notna(row.get("email"))
    ]

    import_id = client.post_contacts_to_mailing_list(
        directory_id, mailing_list_id, contacts
    )
    result = client.wait_for_import(directory_id, mailing_list_id, import_id)
    print(f"Beneficiary import complete: {result}")
    return result


def export_venue_to_qualtrics(
    ds: str,
    directory_id: str,
    mailing_list_id: str,
    client: QualtricsClient,
) -> dict:
    project = os.environ["PROJECT_NAME"]
    env = os.environ["ENV_SHORT_NAME"]
    export_dataset = f"export_{env}"
    raw_dataset = f"raw_{env}"

    sql = f"""
        SELECT
            t.*,
            date_trunc(date("{ds}"), month) as calculation_month,
            current_timestamp() as export_date
        FROM `{project}.{export_dataset}.qualtrics_venue_account` t
    """
    bq = _bq_client()
    df = bq.query(sql).to_dataframe()
    print(f"Venue export: {len(df)} rows")

    _write_audit_table(
        bq, df, project, raw_dataset, "qualtrics_exported_venue_account", ds
    )

    contacts = [
        {
            "email": row["venue_booking_email"],
            "extRef": str(row["venue_id"]),
            "embeddedData": {
                "venue_name": str(row.get("venue_name", "")),
                "venue_type_label": str(row.get("venue_type_label", "")),
                "seniority_days": str(row.get("venue_seniority_days", "")),
                "region": str(row.get("venue_region_name", "")),
                "department": str(row.get("venue_department_code", "")),
                "rural_city_type": str(row.get("venue_rural_city_type", "")),
                "total_real_revenue": str(row.get("total_real_revenue", "")),
                "total_non_cancelled_bookings": str(
                    row.get("total_non_cancelled_bookings", "")
                ),
                "total_created_collective_offers": str(
                    row.get("total_created_collective_offers", "")
                ),
                "is_active_current_year": str(row.get("is_active_current_year", "")),
                "is_individual_active_current_year": str(
                    row.get("is_individual_active_current_year", "")
                ),
                "is_collective_active_current_year": str(
                    row.get("is_collective_active_current_year", "")
                ),
            },
        }
        for _, row in df.iterrows()
        if pd.notna(row.get("venue_booking_email"))
    ]

    import_id = client.post_contacts_to_mailing_list(
        directory_id, mailing_list_id, contacts
    )
    result = client.wait_for_import(directory_id, mailing_list_id, import_id)
    print(f"Venue import complete: {result}")
    return result


def _write_audit_table(
    bq: bigquery.Client,
    df: pd.DataFrame,
    project: str,
    raw_dataset: str,
    table_name: str,
    ds: str,
) -> None:
    partition_date = ds[:7].replace("-", "") + "01"
    table_id = f"{project}.{raw_dataset}.{table_name}${partition_date}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(field="calculation_month"),
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Audit written to {table_id}: {len(df)} rows")
