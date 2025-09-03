
import os
from pathlib import Path

GCP_PROJECT = os.environ.get("PROJECT_NAME", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
BIGQUERY_PPG_DATASET = "tmp_vbusson_dev"
table_prefix = "mrt_external_reporting__"
table_project = "passculture-data-ehp"

REGION_HIERARCHY_TABLE = "region_department"

TAB_NAME_MAX_LENGTH = 31

ALL_TABLES = {
    "individual": {"table_name": f"{table_prefix}individual", "tab": "Part individuelle", "scope": "individual"},
    "collective": {"table_name": f"{table_prefix}collective", "tab": "Part collective", "scope": "collective"},
    "top_individual_offer": {"table_name": f"{table_prefix}top_offer", "tab": "TOP 50 offres - individuel", "scope":"individual"},
    "top_individual_offer_category": {"table_name": f"{table_prefix}top_offer_category", "tab": "TOP 50 offres par catégorie - individuel", "scope":"individual"},
    "top_individual_venue": {"table_name": f"{table_prefix}top_venue", "tab": "TOP 50 lieux - individuel", "scope":"individual"},
}

def build_scope_tables(all_tables: dict[str, str]) -> dict[str, list[str]]:
    indiv = [k for k,v in all_tables.items() if v['scope'] == "individual"]
    coll  = [k for k,v in all_tables.items() if v['scope'] == "collective"]
    return {
        "individual": indiv,
        "collective": coll,
        "all": indiv + coll,
    }

SCOPE_TABLES = build_scope_tables(ALL_TABLES)
# {
#     "individual": ["individual", "top_individual_offer", "top_individual_offer_category", "top_individual_venue"],
#     "collective": ["collective"],
#     "all": ["individual", "collective", "top_individual_offer", "top_individual_offer_category", "top_individual_venue"],
# }

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
for table_key, table_dict in ALL_TABLES.items():
    template = TOP_QUERY_TEMPLATE if table_key.startswith("top_") else BASIC_QUERY_TEMPLATE
    QUERIES[table_key] = template.format(
        project=GCP_PROJECT,
        dataset=BIGQUERY_PPG_DATASET,
        table=table_dict['table_name']
    )


TEMPLATE_DEFAULT = Path("./templates/export_template_v2.xlsx")
REPORT_BASE_DIR_DEFAULT = Path("./reports")




# TOP tables by scope
TOP_TABLES_BY_SCOPE = {
    "individual": [
        "top_individual_offer",
        "top_individual_offer_category",
        "top_individual_venue"
    ],
    "collective": []
}

STAKEHOLDER_REPORTS = {
    "ministere": ["national_summary"],
    "drac": ["regional_summary", "departemental_detail", "academie_detail"]
}


REPORTS = {
    "national_summary": {
        "scales": ["national"],
        "scopes": ["individual", "collective"],
        "lexique": True,
    },
    "regional_summary": {
        "ref_scale": "national",
        "scales": ["regional"],
        "scopes": ["individual", "collective"],
        "lexique": True,
    },
    "departemental_detail": {
        "ref_scale": "regional",
        "scales": ["departemental"],
        "scopes": ["individual"],
        "lexique": True,
    },
    "academie_detail": {
        "ref_scale": "regional",
        "scales": ["academie"],
        "scopes": ["collective"],
        "lexique": True,
    }
}
