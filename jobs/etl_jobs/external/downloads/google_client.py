import logging
import os
import tempfile

import pandas as pd
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import storage
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        logger.info(f"Fetching downloads report for month={month}")

        temp_destination_file_path = self.download_file_from_gcs(
            self.report_bucket_name, file_name, "report.csv"
        )
        try:
            df = pd.read_csv(temp_destination_file_path, encoding="utf-16")
        except Exception as e:
            logger.error(f"Failed to parse CSV {temp_destination_file_path}: {e}")
            raise
        finally:
            if os.path.exists(temp_destination_file_path):
                os.remove(temp_destination_file_path)

        if df.shape[1] != len(OUT_COLS):
            raise ValueError(
                f"Unexpected columns: got {df.shape[1]}, expected {len(OUT_COLS)}"
            )
        df.columns = OUT_COLS
        logger.info(
            f"Downloaded {len(df)} rows for month={month} | "
            f"packages={df['package_name'].nunique()} | "
            f"app_versions={df['app_version_code'].nunique()} | "
            f"total_user_installs={df['total_user_installs'].sum():,}"
        )
        return df

    @staticmethod
    def get_file_path(filename):
        file_name = secure_filename(filename)
        return os.path.join(tempfile.gettempdir(), file_name)

    def download_file_from_gcs(
        self, bucket_name, source_file_name, destination_file_name
    ):
        logger.info(f"Downloading gs://{bucket_name}/{source_file_name}")
        storage_client = storage.Client()
        try:
            bucket = storage_client.get_bucket(bucket_name)
        except NotFound:
            logger.error(f"Bucket not found: {bucket_name}")
            raise
        except GoogleAPIError as e:
            logger.error(f"GCS error accessing bucket {bucket_name}: {e}")
            raise

        blob = bucket.blob(source_file_name)
        if not blob.exists():
            logger.error(
                f"File not found in GCS: gs://{bucket_name}/{source_file_name}"
            )
            raise FileNotFoundError(f"gs://{bucket_name}/{source_file_name}")

        temp_destination_file_path = self.get_file_path(destination_file_name)
        try:
            blob.download_to_filename(temp_destination_file_path)
        except GoogleAPIError as e:
            logger.error(
                f"Failed to download gs://{bucket_name}/{source_file_name}: {e}"
            )
            raise
        logger.info(f"Saved to {temp_destination_file_path}")
        return temp_destination_file_path
