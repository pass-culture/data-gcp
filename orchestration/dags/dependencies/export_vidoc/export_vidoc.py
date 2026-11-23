from typing import Any, Dict, List

from common.config import ENV_SHORT_NAME

S3_SECRET_NAME = f"dbt_export_s3_config_snum_{ENV_SHORT_NAME}"

# Columns exported to the prod bucket: the validated vidoc KPIs (see
# _dbt_docs/kpis), so that new columns can be added to the exp_vidoc__* models
# without being sent to prod. The dev and staging buckets get all the columns of
# these tables, for the VIDOC team to build the next indicators.
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
        # lot 2.1: FI2.3, FI2.4, FI3.1, FI3.2
        *[f"total_{n}plus_category_booked_beneficiaries" for n in (1, 2, 4, 5)],
        *[
            f"total_spent_share_{bucket}_beneficiaries"
            for bucket in ["0"] + [f"{low}_{low + 10}" for low in range(0, 100, 10)]
        ],
        "total_deposit_amount_at_expiration",
        "total_actual_amount_spent_at_expiration",
        "total_distinct_venue_booked_at_expiration",
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
    `columns` (all of them outside prod) are exported to
    `s3://<bucket>/exp_vidoc__<name>/` (bucket resolved from the S3 secret).
    """
    return [
        {
            "dbt_model": f"exp_vidoc__{name}",
            "bigquery_dataset_name": f"export_vidoc_{ENV_SHORT_NAME}",
            "bigquery_table_name": name,
            "columns": columns if ENV_SHORT_NAME == "prod" else ["*"],
            "s3_prefix": f"exp_vidoc__{name}",
        }
        for name, columns in models.items()
    ]


TABLES_CONFIGS = generate_table_configs(VIDOC_MODELS)
