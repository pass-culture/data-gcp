from typing import Any, Dict, List

from common.config import ENV_SHORT_NAME

S3_SECRET_NAME = f"dbt_export_s3_config_snum_{ENV_SHORT_NAME}"

# Columns of the lot 1 vidoc KPIs (see _dbt_docs/kpis): only these are
# exported, so that new columns can be added to the exp_vidoc__* models without
# being sent to the S3 bucket.
VIDOC_MODELS = {
    "diversity": [
        "deposit_expiration_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "total_expired_deposit_beneficiaries",
        "total_3plus_category_booked_beneficiaries",
    ],
    "diversity_by_category": [
        "deposit_expiration_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "offer_category_id",
        "total_category_booked_beneficiaries",
    ],
    "beneficiary": [
        "partition_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "age_at_calculation",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "total_beneficiaries",
        "total_actual_beneficiaries",
    ],
    "population_coverage": [
        "partition_month",
        "birth_month",
        "milestone_age",
        "department_code",
        "department_name",
        "region_name",
        "region_code",
        "total_population_last_12_months",
    ],
    "beneficiary_coverage": [
        "partition_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "milestone_age",
        "total_beneficiaries_last_12_months",
    ],
}


# Lot 2.1 (diversity and credit use at expiration: FI2.3, FI2.4, FI3.1, FI3.2):
# columns exported to the dev and staging buckets only, so that the VIDOC team
# can build the dashboards before the prod release. Move them into VIDOC_MODELS
# once the indicators are validated.
VIDOC_PREVIEW_COLUMNS = {
    "diversity": [
        "total_1plus_category_booked_beneficiaries",
        "total_2plus_category_booked_beneficiaries",
        "total_4plus_category_booked_beneficiaries",
        "total_5plus_category_booked_beneficiaries",
        "total_spent_share_0_beneficiaries",
        "total_spent_share_0_25_beneficiaries",
        "total_spent_share_25_50_beneficiaries",
        "total_spent_share_50_75_beneficiaries",
        "total_spent_share_75_90_beneficiaries",
        "total_spent_share_90_100_beneficiaries",
        "total_deposit_amount_at_expiration",
        "total_actual_amount_spent_at_expiration",
        "total_distinct_venue_booked_at_expiration",
    ],
}


def merge_preview_columns(
    models: Dict[str, List[str]],
    preview_columns: Dict[str, List[str]],
    env_short_name: str,
) -> Dict[str, List[str]]:
    """Add the preview columns to the exported models, outside prod only."""
    if env_short_name == "prod":
        return models
    return {
        name: columns + [c for c in preview_columns.get(name, []) if c not in columns]
        for name, columns in models.items()
    }


def generate_table_configs(models: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """
    dbt model `exp_vidoc__<name>` is aliased in BigQuery to
    `export_vidoc_{env}.<name>` (see `generate_alias_name` macro), then its
    `columns` are exported to `s3://<bucket>/exp_vidoc__<name>/` (bucket
    resolved from the S3 secret).
    """
    return [
        {
            "dbt_model": f"exp_vidoc__{name}",
            "bigquery_dataset_name": f"export_vidoc_{ENV_SHORT_NAME}",
            "bigquery_table_name": name,
            "columns": columns,
            "s3_prefix": f"exp_vidoc__{name}",
        }
        for name, columns in models.items()
    ]


TABLES_CONFIGS = generate_table_configs(
    merge_preview_columns(VIDOC_MODELS, VIDOC_PREVIEW_COLUMNS, ENV_SHORT_NAME)
)
