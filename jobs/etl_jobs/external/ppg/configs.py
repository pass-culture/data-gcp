
import os
from pathlib import Path

GCP_PROJECT = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
BIGQUERY_PPG_DATASET = "tmp_vbusson_dev"
table_prefix = "mrt_external_reporting__"
table_project = "passculture-data-ehp"


# todo add tops indivitual and tops collective tables
# BQ_TABLES = {
#     "individual": f"{table_prefix}individual",
#     "collective": f"{table_prefix}collective",
# }

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
    "individual": ["individual"],#"top_offer","top_offer_category","top_venue"], # add tops_individual when available
    "collective": ["collective"], # add tops_collective when available
    "all": ["individual", "collective"], # add tops when available
}

TOP_TABLES = {
    "individual": ["offer","offer_category","venue"],
    "collective": [],
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
    FROM `{GCP_PROJECT}.{BIGQUERY_PPG_DATASET}.{table_prefix}{table}`
    WHERE 1=1
    ORDER BY partition_month, dimension_name, dimension_value, kpi_name
    """
    for scope,tables in SCOPE_TABLES.items() for table in tables
} | {
    f"top_{scope}_{table}": f"""
    SELECT
        *
    FROM `{GCP_PROJECT}.{BIGQUERY_PPG_DATASET}.{table_prefix}top_{table}`
    WHERE 1=1
    ORDER BY partition_month, dimension_name, dimension_value, kpi_name
    """
    for scope,tables in TOP_TABLES.items() for table in tables
}

ALL_TABLES = {
    "individual": f"{table_prefix}individual",
    "collective": f"{table_prefix}collective", 
    "top_individual_offer": f"{table_prefix}top_offer",
    "top_individual_offer_category": f"{table_prefix}top_offer_category",
    "top_individual_venue": f"{table_prefix}top_venue",
}

# Scope to table mapping
SCOPE_TABLES = {
    "individual": ["individual", "top_individual_offer", "top_individual_offer_category", "top_individual_venue"],
    "collective": ["collective"], 
    "all": ["individual", "collective", "top_individual_offer", "top_individual_offer_category", "top_individual_venue"],
}

# Generate queries for each table
BASIC_QUERY_TEMPLATE = """
SELECT partition_month, updated_at, dimension_name, dimension_value,
       kpi_name, numerator, denominator, kpi
FROM `{project}.{dataset}.{table}`
WHERE 1=1
ORDER BY partition_month, dimension_name, dimension_value, kpi_name
"""

TOP_QUERY_TEMPLATE = """
SELECT *
FROM `{project}.{dataset}.{table}` 
WHERE 1=1
ORDER BY partition_month, dimension_name, dimension_value
"""

QUERIES = {}
for table_key, table_name in ALL_TABLES.items():
    template = TOP_QUERY_TEMPLATE if table_key.startswith("top_") else BASIC_QUERY_TEMPLATE
    QUERIES[table_key] = template.format(
        project=GCP_PROJECT,
        dataset=BIGQUERY_PPG_DATASET, 
        table=table_name
    )


TEMPLATE_DEFAULT = Path("./templates/export_template_v2.xlsx")
REPORT_BASE_DIR_DEFAULT = Path("./reports")