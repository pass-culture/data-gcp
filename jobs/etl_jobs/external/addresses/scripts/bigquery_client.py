from google.cloud import bigquery


class BigQueryClient:
    def query(self, query_string):
        return bigquery.Client().query(query_string).to_dataframe()
