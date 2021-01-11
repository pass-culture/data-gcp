from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryClient:
    def __init__(self, credential_path):
        self.credential_path = credential_path

    def get_client(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.credential_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(credentials=credentials, project=credentials.project_id)

    def query(self, query_string):
        return self.get_client().query(query_string).to_dataframe()
