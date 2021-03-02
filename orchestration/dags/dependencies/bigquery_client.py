from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryClient:
    def query(self, query_string):
        return bigquery.Client().query(query_string).to_dataframe()
