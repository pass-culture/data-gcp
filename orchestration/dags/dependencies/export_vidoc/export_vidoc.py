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


TABLES_CONFIGS = generate_table_configs(VIDOC_MODELS)
