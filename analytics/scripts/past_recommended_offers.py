from google.cloud import bigquery

GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCP_BIGQUERY_DATASET = "algo_reco_kpi_data"

client = bigquery.Client()

table_id = f"{GCP_PROJECT_ID}.{GCP_BIGQUERY_DATASET}.past_recommended_offers"

schema = [
    bigquery.SchemaField("userid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("offerid", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)
