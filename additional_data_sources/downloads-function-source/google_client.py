import os

import pandas as pd
import tempfile
from werkzeug.utils import secure_filename

from google.cloud import storage

OUT_COLS = [
    "date",
    "package_name",
    "app_version_code",
    "daily_device_installs",
    "daily_device_uninstalls",
    "daily_device_upgrades",
    "total_user_installs",
    "daily_user_installs",
    "daily_user_uninstalls",
    "active_device_installs",
    "install_events",
    "update_events",
    "uninstall_events",
]


class GoogleClient:
    def __init__(self, report_bucket_name):
        self.report_bucket_name = report_bucket_name

    def get_downloads(self, month="2021-05"):
        file_name = f"stats/installs/installs_app.passculture.webapp_{month.replace('-', '')}_app_version.csv"

        temp_destination_file_path = self.download_file_from_gcs(
            self.report_bucket_name, file_name, "report.csv"
        )
        df = pd.read_csv(temp_destination_file_path, encoding="utf-16")
        df.columns = OUT_COLS
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
