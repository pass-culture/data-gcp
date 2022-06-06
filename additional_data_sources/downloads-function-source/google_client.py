import os

import pandas as pd
import tempfile
from werkzeug.utils import secure_filename

from google.cloud import storage


class GoogleClient:
    def __init__(self, report_bucket_name):
        self.report_bucket_name = report_bucket_name

    def get_downloads(self, month="2021-05"):
        file_name = f"stats/installs/installs_app.passculture.webapp_{month.replace('-', '')}_app_version.csv"

        temp_destination_file_path = self.download_file_from_gcs(
            self.report_bucket_name, file_name, "report.csv"
        )
        df = pd.read_csv(temp_destination_file_path, encoding="utf-16")
        print(df.columns)
        os.remove(temp_destination_file_path)
        return df

    @staticmethod
    def get_file_path(filename):
        file_name = secure_filename(filename)
        return os.path.join(tempfile.gettempdir(), file_name)

    def download_file_from_gcs(
        self, bucket_name, source_file_name, destination_file_name
    ):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(source_file_name)
        temp_destination_file_path = self.get_file_path(destination_file_name)
        blob.download_to_filename(temp_destination_file_path)
        return temp_destination_file_path
