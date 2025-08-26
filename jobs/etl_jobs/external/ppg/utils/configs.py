
import os
from pathlib import Path

GCP_PROJECT = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
BIGQUERY_PPG_DATASET = "tmp_vbusson_dev"
table_prefix = "mrt_external_reporting__"
table_project = "passculture-data-ehp"


# todo add tops indivitual and tops collective tables
BQ_TABLES = {
    "individual": f"{table_prefix}individual",
    "collective": f"{table_prefix}collective",
}

# kpi_schema = {
#     "partition_month": "DATE",
#     "updated_at": "TIMESTAMP",
#     "dimension_name": "STRING",
#     "dimension_value": "STRING",
#     "kpi_name": "STRING",
#     "numerator": "FLOAT",
#     "denominator":"FLOAT",
#     "kpi": "FLOAT",
# }
SCOPE_TABLES = {
    "individual": ["individual"], # add tops_individual when available
    "collective": ["collective"], # add tops_collective when available
    "all": ["individual", "collective"], # add tops when available
}

QUERIES = {
    scope: f"""
    SELECT
        partition_month,
        updated_at,
        dimension_name,
        dimension_value,
        kpi_name,
        numerator,
        denominator,
        kpi
    FROM `{GCP_PROJECT}.{BIGQUERY_PPG_DATASET}.{table}`
    WHERE 1=1
    ORDER BY partition_month, dimension_name, dimension_value, kpi_name
    """
    for scope,table, in BQ_TABLES.items()
}



TEMPLATE_DEFAULT = Path("./templates/export_template_v2.xlsx")
REPORT_BASE_DIR_DEFAULT = Path("./reports")