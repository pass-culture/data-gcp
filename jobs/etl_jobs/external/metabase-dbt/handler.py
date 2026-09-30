import http.client
import logging
import os
import time
from collections import Counter
from typing import List, Optional

import pandas as pd
import requests
from dbtmetabase import DbtMetabase, Filter
from dbtmetabase._models import _build_tables
from dbtmetabase.errors import MetabaseStateError
from google.auth.transport.requests import Request
from google.cloud import storage
from google.oauth2 import id_token

from utils import CLIENT_ID, METABASE_API_KEY, METABASE_HOST

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _redact_token(token):
    if not token:
        return f"<EMPTY token type={type(token).__name__}>"
    return f"len={len(token)} prefix={token[:10]}... suffix=...{token[-6:]}"


def _redact_headers(headers):
    if not headers:
        return {}
    redacted = {}
    for k, v in headers.items():
        if k.lower() in ("authorization", "proxy-authorization", "x-api-key"):
            token = v.removeprefix("Bearer ").strip() if isinstance(v, str) else v
            redacted[k] = _redact_token(token)
        else:
            redacted[k] = v
    return redacted


if os.environ.get("DEBUG_HTTP") == "1":
    http.client.HTTPConnection.debuglevel = 1
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("requests.packages.urllib3").setLevel(logging.DEBUG)
    logger.warning(
        "DEBUG_HTTP=1 — HTTP wire logging is ON (request headers will be visible)"
    )


def _introspect_dbtmetabase_http(client, max_depth=2):
    """Dump where the requests.Session lives inside the dbtmetabase client and
    log its headers (redacted). Walks the object graph up to max_depth."""
    logger.info("DbtMetabase MRO: %s", [c.__name__ for c in type(client).__mro__])
    seen = set()
    found_any = False

    def walk(obj, path, depth):
        nonlocal found_any
        if id(obj) in seen or depth > max_depth:
            return
        seen.add(id(obj))

        if isinstance(obj, requests.Session):
            found_any = True
            logger.info(
                "Found requests.Session at %s — headers=%s",
                path or "<client>",
                _redact_headers(dict(obj.headers)),
            )
            return

        attrs = getattr(obj, "__dict__", {})
        if depth == 0:
            logger.info(
                "%s attrs: %s",
                path or "<client>",
                {k: type(v).__name__ for k, v in attrs.items()},
            )

        for name, value in attrs.items():
            if name.startswith("__"):
                continue
            sub_path = f"{path}.{name}" if path else name
            if isinstance(value, requests.Session):
                found_any = True
                logger.info(
                    "Found requests.Session at .%s — headers=%s",
                    sub_path,
                    _redact_headers(dict(value.headers)),
                )
            elif hasattr(value, "headers") and hasattr(value, "request"):
                found_any = True
                logger.info(
                    "Session-like at .%s (type=%s) headers=%s",
                    sub_path,
                    type(value).__name__,
                    _redact_headers(dict(getattr(value, "headers", {}))),
                )
            elif hasattr(value, "__dict__") and not isinstance(
                value, (str, bytes, int, float, bool, list, tuple, dict, set)
            ):
                walk(value, sub_path, depth + 1)

    walk(client, "", 0)

    if not found_any:
        logger.warning(
            "No requests.Session found within depth=%d. "
            "dbt-metabase likely creates a Session per call — "
            "http_headers may not be persisted. Inspect dir(client): %s",
            max_depth,
            sorted(a for a in dir(client) if not a.startswith("__"))[:60],
        )


class MetabaseExportError(Exception):
    """Raised when the dbt -> Metabase export finished with errors."""


class _ExportReport(logging.Handler):
    """Collects dbtmetabase log records during an export to build a recap,
    and hides the per-field "is up to date" lines (thousands per run)."""

    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.counts = Counter()
        self.warnings: List[str] = []
        self.errors: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        # Attached as a handler on "dbtmetabase": collect warnings and errors.
        if record.levelno >= logging.ERROR:
            self.errors.append(record.getMessage())
        elif record.levelno >= logging.WARNING:
            self.warnings.append(record.getMessage())

    def count_and_drop(self, record: logging.LogRecord) -> bool:
        # Attached as a filter on "dbtmetabase._models": count progress lines
        # and drop the noisy "is up to date" ones.
        message = record.getMessage()
        if message.endswith("is up to date"):
            self.counts["up_to_date"] += 1
            return False
        if "will be updated" in message:
            self.counts["planned"] += 1
        elif "updated successfully" in message:
            self.counts["applied"] += 1
        return True

    def log_recap(self) -> None:
        lines = [
            "=" * 30 + " Metabase export recap " + "=" * 30,
            f"Up to date (tables + fields): {self.counts['up_to_date']}",
            f"Updates planned: {self.counts['planned']} / applied: {self.counts['applied']}",
            f"Warnings: {len(self.warnings)}",
            *(f"  - {w}" for w in self.warnings),
            f"Errors: {len(self.errors)}",
            *(f"  - {e}" for e in self.errors),
        ]
        if any("does not exist" in e for e in self.errors):
            lines.append(
                "Hint: a table/field 'does not exist' when dbt documents it but "
                "Metabase has not synced it. Either the column was added recently "
                "and Metabase's schema sync is still running (re-run later), or the "
                "dbt yml documents a column the model does not output (fix the yml)."
            )
        lines.append("=" * 83)
        log = logger.error if self.errors else logger.info
        log("\n".join(lines))


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
        # TODO(auth-migration): Replace usage of google.oauth2.id_token with standard JWT validation.
        # Owner: vbusson
        # Date: 2026-05-22

        open_id_token = self.get_open_id(CLIENT_ID)
        if not open_id_token:
            raise ValueError("OpenID token is empty — IAP authentication will fail")

        logger.info(
            "Fetched OpenID token for authentication: %s",
            _redact_token(open_id_token),
        )

        # Download DBT manifest from GCS
        self.download_gcs_dbt_manifest(
            airflow_bucket_name,
            blob_name=airflow_bucket_manifest_path,
            destination_file_path=local_manifest_path,
        )
        logger.info(f"Downloaded DBT manifest from GCS: {local_manifest_path}")

        http_headers = {"Proxy-Authorization": f"Bearer {open_id_token}"}
        logger.info(
            "Initializing DbtMetabase: host=%s api_key=%s http_headers=%s",
            METABASE_HOST,
            _redact_token(METABASE_API_KEY),
            _redact_headers(http_headers),
        )
        self.client = DbtMetabase(
            manifest_path=local_manifest_path,
            metabase_url=METABASE_HOST,
            metabase_api_key=METABASE_API_KEY,
            http_headers=http_headers,
        )

        _introspect_dbtmetabase_http(self.client)

        logger.info("Initialized DbtMetabase client.")

    def get_open_id(self, client_id: str) -> str:
        """
        DEPRECATED !!!
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

    def wait_for_metabase_sync(
        self,
        metabase_database: str,
        schema_filters: List[str],
        model_names: List[str],
        timeout: int,
        poll_interval: int = 15,
    ) -> None:
        """
        Triggers a Metabase schema sync and waits until every documented column
        of the selected dbt models is known by Metabase, or until timeout.

        dbt-metabase can do this itself (sync_timeout), but it raises on timeout
        and then exports nothing. Here we only warn, so the export still runs and
        the recap lists what is still missing.
        """
        metabase = self.client.metabase
        database = metabase.find_database(name=metabase_database)
        if not database:
            raise MetabaseExportError(
                f"Metabase database not found: {metabase_database}"
            )

        schema_filter = Filter(include=schema_filters)
        model_filter = Filter(include=model_names)
        expected = {
            f"{m.schema.upper()}.{m.alias.upper()}": {c.name.upper() for c in m.columns}
            for m in self.client.manifest.read_models()
            if schema_filter.match(m.schema) and model_filter.match(m.name)
        }

        logger.info(
            "Triggering Metabase schema sync on '%s' and waiting up to %ss for %s tables",
            metabase_database,
            timeout,
            len(expected),
        )
        metabase.sync_database_schema(database["id"])

        deadline = time.monotonic() + timeout
        while True:
            tables = _build_tables(metabase.get_database_metadata(database["id"]))
            missing = []
            for table_key, columns in expected.items():
                fields = tables.get(table_key, {}).get("fields")
                if fields is None:
                    missing.append(table_key)
                else:
                    missing += [
                        f"{table_key}.{c}" for c in sorted(columns - fields.keys())
                    ]

            if not missing:
                logger.info("Metabase schema is in sync with dbt manifest.")
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "Metabase schema still missing %s tables/fields after %ss: %s",
                    len(missing),
                    timeout,
                    missing,
                )
                return
            logger.info(
                "Waiting for Metabase sync, %s tables/fields missing (e.g. %s)",
                len(missing),
                missing[:3],
            )
            time.sleep(poll_interval)

    def export_model(
        self,
        metabase_database: str,
        schema_filters: List[str],
        model_names: List[str],
        docs_url: str = None,
        sync_timeout: int = 600,
    ) -> None:
        """
        Exports the DBT models to Metabase, appending tags and filtering by specified models.

        Args:
            model_names (List[str]): A list of model names to export to Metabase.
            sync_timeout (int): Seconds to wait for Metabase to sync new tables/columns
                before exporting. 0 skips the wait.
        """
        if sync_timeout:
            self.wait_for_metabase_sync(
                metabase_database, schema_filters, model_names, timeout=sync_timeout
            )

        logger.info(f"Exporting models to Metabase: {model_names}")
        report = _ExportReport()
        dbtmetabase_logger = logging.getLogger("dbtmetabase")
        models_logger = logging.getLogger("dbtmetabase._models")
        dbtmetabase_logger.addHandler(report)
        models_logger.addFilter(report.count_and_drop)
        try:
            self.client.export_models(
                metabase_database=metabase_database,
                schema_filter=Filter(include=schema_filters),
                model_filter=Filter(include=model_names),
                append_tags=False,
                # Sync already awaited above; dbt-metabase would raise on timeout.
                sync_timeout=0,
                docs_url=docs_url,
            )
        except MetabaseStateError:
            # "Non-critical errors encountered": the errors were logged (and
            # collected in the report) and all valid updates were still applied.
            if not report.errors:
                raise
        finally:
            dbtmetabase_logger.removeHandler(report)
            models_logger.removeFilter(report.count_and_drop)

        report.log_recap()
        if report.errors:
            raise MetabaseExportError(
                f"Metabase export finished with {len(report.errors)} error(s), see recap above"
            )
        logger.info(f"Models {model_names} exported to Metabase successfully.")

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
