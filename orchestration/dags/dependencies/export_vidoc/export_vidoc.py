from typing import Any, Dict, List

from common.config import ENV_SHORT_NAME

S3_SECRET_NAME = f"dbt_export_s3_config_snum_{ENV_SHORT_NAME}"

VIDOC_MODELS = [
    "diversity",
    "diversity_by_category",
    "beneficiary",
    "population_coverage",
    "beneficiary_coverage",
]


def generate_table_configs(models: List[str]) -> List[Dict[str, Any]]:
    """
    dbt model `exp_vidoc__<name>` is aliased in BigQuery to
    `export_vidoc_{env}.<name>` (see `generate_alias_name` macro), then
    exported to `s3://<bucket>/exp_vidoc__<name>/` (bucket resolved from the
    S3 secret).
    """
    return [
        {
            "dbt_model": f"exp_vidoc__{name}",
            "bigquery_dataset_name": f"export_vidoc_{ENV_SHORT_NAME}",
            "bigquery_table_name": name,
            "s3_prefix": f"exp_vidoc__{name}",
        }
        for name in models
    ]


TABLES_CONFIGS = generate_table_configs(VIDOC_MODELS)
