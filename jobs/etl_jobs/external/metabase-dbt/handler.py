from typing import List

from dbtmetabase import DbtMetabase, Filter
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import id_token

from utils import CLIENT_ID, METABASE_API_KEY, METABASE_HOST


class MetabaseDBTHandler:
    def get_open_id(self, client_id):
        return id_token.fetch_id_token(Request(), client_id)

    def download_gcs_dbt_manifest(
        self, bucket_name: str, blob_name: str, destination_file_path: str
    ):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(destination_file_path)
        return

    def __init__(self, composer_bucket_name: str, manifest_path: str):
        open_id_token = self.get_open_id(CLIENT_ID)
        self.download_gcs_dbt_manifest(
            composer_bucket_name,
            blob_name=manifest_path,
            destination_file_path=manifest_path,
        )
        self.client = DbtMetabase(
            manifest_path=manifest_path,
            metabase_url=METABASE_HOST,
            metabase_api_key=METABASE_API_KEY,
            http_headers={"Proxy-Authorization": f" Bearer {open_id_token}"},
        )

    def export_model(self, model_names: List):
        self.client.export_models(
            metabase_database="Analytics",
            schema_filter=Filter(include=["analytics_prod"]),
            model_filter=Filter(include=model_names),
            append_tags=True,
            docs_url=None,  # TODO once we will have a dedicated uri
        )
