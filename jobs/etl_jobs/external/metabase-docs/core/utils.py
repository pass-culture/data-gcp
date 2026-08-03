import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

BIGQUERY_RAW_DATASET = f"raw_{ENV_SHORT_NAME}"
BIGQUERY_INT_METABASE_DATASET = f"int_metabase_{ENV_SHORT_NAME}"

NOTION_DOCS_TABLE = "notion_dashboard_docs"
ASSET_CATALOG_TABLE = "asset_catalog"
DASHBOARD_CARD_TABLE = "metabase_report_dashboard_card"
QUERY_EXECUTION_TABLE = "metabase_query_execution"
QUERY_TABLE = "metabase_query"
DASHBOARD_DOCS_TABLE = "dashboard_documentation"

VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "europe-west1")
VERTEX_MODEL = os.environ.get("VERTEX_MODEL", "gemini-2.5-flash")
SQL_SNIPPET_CARDS = int(os.environ.get("SQL_SNIPPET_CARDS", "4"))
