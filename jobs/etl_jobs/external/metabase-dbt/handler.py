import logging
from typing import List, Optional

import pandas as pd
from dbtmetabase import DbtMetabase, Filter
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import id_token

from utils import CLIENT_ID, METABASE_API_KEY, METABASE_HOST

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigqueryDBTHandler:
    """
    A handler class for fetching data from BigQuery and processing it for Metabase queries.

    Methods:
    --------
    get_bigquery_metabase_collection(dataset_name: str, table_name: str, limit: Optional[int] = 1) -> pd.DataFrame:
        Fetches data from a BigQuery table and returns it as a pandas DataFrame, ordered by total Metabase queries.
        If `limit` is None, all data will be fetched.
    """

    def __init__(self):
        # Set up logging
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def get_bq_metabase_internal_exposure(
        self, dataset_name: str, table_name: str, limit: Optional[int] = 1
    ) -> pd.DataFrame:
        """
        Fetches data from a BigQuery table and returns it as a pandas DataFrame. The data is ordered by `total_metabase_queries`.

        Args:
        -----
        dataset_name: str
            The name of the BigQuery dataset.
        table_name: str
            The name of the table to fetch data from.
        limit: Optional[int], default 1
            The maximum number of rows to fetch. If None, no limit is applied.

        Returns:
        --------
        pd.DataFrame
            A pandas DataFrame containing the fetched data.
        """

        try:
            query = f"SELECT * FROM {dataset_name}.{table_name} ORDER BY total_metabase_queries DESC"

            if limit is not None:
                query += f" LIMIT {limit}"

            self.logger.info(f"Executing query: {query}")

            df = pd.read_gbq(query)
            self.logger.info(f"Fetched {len(df)} rows from {dataset_name}.{table_name}")
            return df

        except Exception as e:
            self.logger.error(
                f"Error fetching data from {dataset_name}.{table_name}: {e}"
            )
            raise


class MetabaseDBTHandler:
    def __init__(
        self,
        airflow_bucket_name: str,
        airflow_bucket_manifest_path: str,
        local_manifest_path: str,
    ):
        """
        Initializes the MetabaseDBTHandler class by downloading the DBT manifest
        from Google Cloud Storage and setting up the DbtMetabase client.

        Args:
            airflow_bucket_name (str): The name of the GCS bucket where the DBT manifest is stored.
            airflow_bucket_manifest_path (str): The path to the DBT manifest in the GCS bucket.
            local_manifest_path (str): The path to the DBT manifest on the local filesystem.
        """
        open_id_token = self.get_open_id(CLIENT_ID)
        logger.info("Fetched OpenID token for authentication.")

        # Download DBT manifest from GCS
        self.download_gcs_dbt_manifest(
            airflow_bucket_name,
            blob_name=airflow_bucket_manifest_path,
            destination_file_path=local_manifest_path,
        )
        logger.info(f"Downloaded DBT manifest from GCS: {local_manifest_path}")

        # Initialize the DbtMetabase client
        self.client = DbtMetabase(
            manifest_path=local_manifest_path,
            metabase_url=METABASE_HOST,
            metabase_api_key=METABASE_API_KEY,
            http_headers={"Proxy-Authorization": f"Bearer {open_id_token}"},
        )
        logger.info("Initialized DbtMetabase client.")

    def get_open_id(self, client_id: str) -> str:
        """
        Fetches the OpenID token using the provided client_id.

        Args:
            client_id (str): The client ID used to fetch the OpenID token.

        Returns:
            str: The OpenID token.
        """
        logger.info("Fetching OpenID token.")
        try:
            token = id_token.fetch_id_token(Request(), client_id)
            logger.info("OpenID token fetched successfully.")
            return token
        except Exception as e:
            logger.error(f"Failed to fetch OpenID token: {e}")
            raise

    def download_gcs_dbt_manifest(
        self, bucket_name: str, blob_name: str, destination_file_path: str
    ) -> None:
        """
        Downloads the DBT manifest file from Google Cloud Storage.

        Args:
            bucket_name (str): The name of the GCS bucket.
            blob_name (str): The name of the DBT manifest blob in the bucket.
            destination_file_path (str): The local file path where the manifest will be saved.
        """
        logger.info(
            f"Downloading DBT manifest from GCS bucket {bucket_name}, blob {blob_name}."
        )
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename(destination_file_path)
            logger.info(f"Manifest downloaded to {destination_file_path}.")
        except Exception as e:
            logger.error(f"Failed to download DBT manifest: {e}")
            raise

    def export_model(
        self,
        metabase_database: str,
        schema_filters: List[str],
        model_names: List[str],
        docs_url: str = None,
    ) -> None:
        """
        Exports the DBT models to Metabase, appending tags and filtering by specified models.

        Args:
            model_names (List[str]): A list of model names to export to Metabase.
        """
        logger.info(f"Exporting models to Metabase: {model_names}")
        try:
            self.client.export_models(
                metabase_database=metabase_database,
                schema_filter=Filter(include=schema_filters),
                model_filter=Filter(include=model_names),
                append_tags=False,
                sync_timeout=0,
                docs_url=docs_url,
            )
            logger.info(f"Models {model_names} exported to Metabase successfully.")
        except Exception as e:
            logger.error(f"Failed to export models to Metabase: {e}")
            pass

    def export_exposures(self, collection_filters: List[str], output_path: str) -> None:
        """
        Exports the DBT exposures to a specified path.

        Args:
            collection_filters (List[str]): A list of exposure collection filters to apply.
            output_path (str): The path where the exposures will be saved.
        """
        logger.info(
            f"Exporting exposures with collection filters: {collection_filters}"
        )
        try:
            self.client.extract_exposures(
                output_path=output_path,
                collection_filter=Filter(include=collection_filters),
            )
            logger.info(f"Exposures exported to {output_path} successfully.")
        except Exception as e:
            logger.error(f"Failed to export exposures: {e}")
            raise

    def push_exposures_to_bucket(
        self,
        airflow_bucket_name: str,
        airflow_bucket_path: str,
        exposure_local_path: str,
    ) -> None:
        """
        Pushes the exported exposures to the specified GCS bucket.

        Args:
            airflow_bucket_name (str): The name of the GCS bucket where the exposures will be saved.
            airflow_bucket_path (str): The base folder in the GCS bucket to save the exposures.
            exposure_local_path (str): The path where the exposures are saved locally.
        """
        logger.info(
            f"Pushing exposures to GCS bucket {airflow_bucket_name}, folder {airflow_bucket_path}"
        )
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(airflow_bucket_name)
            blob = bucket.blob(airflow_bucket_path)
            blob.upload_from_filename(exposure_local_path)
            logger.info(
                f"Exposures pushed to GCS bucket {airflow_bucket_name} successfully."
            )
        except Exception as e:
            logger.error(f"Failed to push exposures to GCS bucket: {e}")
            raise
