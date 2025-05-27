import os
import pandas as pd
import datetime
import re
import requests
import logging
from typing import List, Tuple, Optional, Dict, Set, Annotated
from enum import Enum
from google.cloud import bigquery
from formatting import format_slack_message
from pydantic import BaseModel, Field, field_validator, ConfigDict, computed_field

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

GCP_PROJECT = os.environ.get("GCP_PROJECT_ID")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

today = datetime.datetime.now()
days3 = datetime.timedelta(days=3)
days7 = datetime.timedelta(days=7)
month = datetime.timedelta(weeks=4)
year = datetime.timedelta(days=365)

schedule_mapping = {
    "daily": today - days3,
    "weekly": today - days7,
    "monthly": today - month,
    "yearly": today - year,
    "default": today - month,
}


class ScheduleTag(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    DEFAULT = "default"


class TableInfo(BaseModel):
    """Information about a BigQuery table."""

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True,
    )

    schema: str = Field(..., description="Table schema name")
    name: str = Field(..., description="Table name")
    last_modified_time: datetime.datetime = Field(
        ..., description="Last modification timestamp"
    )
    schedule_tag: ScheduleTag = Field(..., description="Expected update schedule")
    is_partition_table: bool = Field(
        ..., description="Whether the table is partitioned"
    )

    @computed_field
    @property
    def full_name(self) -> str:
        """Get the full table name in format 'schema.table'."""
        return f"{self.schema}.{self.name}"

    @computed_field
    @property
    def days_since_update(self) -> int:
        """Get the number of days since the last update."""
        return (datetime.datetime.now() - self.last_modified_time).days


class AlertConfig(BaseModel):
    """Configuration for the BigQuery alerts."""

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        str_strip_whitespace=True,
    )

    project_id: str = Field(..., description="GCP project ID")
    env_short_name: str = Field(
        ..., description="Environment short name (prod, stg, dev)"
    )
    webhook_url: Optional[str] = Field(None, description="Slack webhook URL for alerts")
    archive_days_threshold: int = Field(
        14, ge=1, description="Number of days before archiving a table"
    )
    archive_dataset: Optional[str] = Field(
        None, description="Dataset to archive tables to"
    )
    delete_instead_of_archive: bool = Field(
        False, description="Delete tables instead of archiving them"
    )
    skip_slack_alerts: bool = Field(False, description="Skip sending Slack alerts")

    @field_validator("env_short_name")
    @classmethod
    def validate_env_short_name(cls, v: str) -> str:
        allowed_envs = {"prod", "stg", "dev"}
        if v not in allowed_envs:
            raise ValueError(f"env_short_name must be one of {allowed_envs}")
        return v

    @field_validator("archive_days_threshold")
    @classmethod
    def validate_archive_days(cls, v: int) -> int:
        if v < 1:
            raise ValueError("archive_days_threshold must be at least 1")
        return v

    @computed_field
    @property
    def env_emoji(self) -> str:
        """Get the environment emoji for alerts."""
        return "🚨" if self.env_short_name == "prod" else "🔧"


class BigQueryAlerts:
    """Main class for handling BigQuery table freshness alerts."""

    def __init__(self, config: AlertConfig):
        self.config = config
        self._schedule_mapping = self._create_schedule_mapping()
        self._bq_client = bigquery.Client(project=config.project_id)
        logger.info("Initialized BigQueryAlerts with config: %s", config)

    def _create_schedule_mapping(self) -> Dict[ScheduleTag, datetime.datetime]:
        """Create mapping of schedule tags to their expected update times."""
        now = datetime.datetime.now()
        return {
            ScheduleTag.DAILY: now - datetime.timedelta(days=3),
            ScheduleTag.WEEKLY: now - datetime.timedelta(days=7),
            ScheduleTag.MONTHLY: now - datetime.timedelta(weeks=4),
            ScheduleTag.YEARLY: now - datetime.timedelta(days=365),
            ScheduleTag.DEFAULT: now - datetime.timedelta(weeks=4),
        }

    def _archive_table(self, table: TableInfo) -> None:
        """Archive or delete a table that hasn't been updated for too long."""
        source_table = f"{self.config.project_id}.{table.schema}.{table.name}"

        if self.config.delete_instead_of_archive:
            try:
                logger.info("Deleting table %s", table.full_name)
                self._bq_client.delete_table(source_table)
                logger.info("Successfully deleted table %s", table.full_name)
            except Exception as e:
                logger.error("Error deleting table %s: %s", table.full_name, str(e))
                raise
            return

        if not self.config.archive_dataset:
            logger.warning(
                "No archive dataset configured, skipping archiving for %s",
                table.full_name,
            )
            raise ValueError("No archive dataset configured")

        try:
            archive_date = datetime.datetime.now().strftime("%Y%m%d")
            archive_table_name = f"{archive_date}_{table.schema}_{table.name}"
            archive_table_id = f"{self.config.project_id}.{self.config.archive_dataset}.{archive_table_name}"

            logger.info("Archiving table %s to %s", table.full_name, archive_table_id)

            # Copy the table to the archive dataset
            job_config = bigquery.CopyJobConfig()
            self._bq_client.copy_table(
                source_table, archive_table_id, job_config=job_config
            )

            # Delete the original table
            self._bq_client.delete_table(source_table)

            logger.info(
                "Successfully archived table %s to %s",
                table.full_name,
                archive_table_id,
            )
        except Exception as e:
            logger.error("Error archiving table %s: %s", table.full_name, str(e))
            raise

    def get_datasets_to_scan(self) -> Set[str]:
        """Get the list of datasets to scan for freshness checks."""
        query = f"""
        SELECT DISTINCT schema_name
        FROM {self.config.project_id}.`region-europe-west1`.INFORMATION_SCHEMA.SCHEMATA;
        """
        df = pd.read_gbq(query)
        datasets = set(df["schema_name"].tolist())

        target_prefixes = {"int_", "ml_"}
        target_datasets = {
            f"analytics_{self.config.env_short_name}",
            f"clean_{self.config.env_short_name}",
            f"raw_{self.config.env_short_name}",
            f"raw_applicative_{self.config.env_short_name}",
            f"snapshot_{self.config.env_short_name}",
            f"backend_{self.config.env_short_name}",
            f"appsflyer_import_{self.config.env_short_name}",
        }

        filtered_datasets = {
            dataset
            for dataset in datasets
            if any(dataset.startswith(prefix) for prefix in target_prefixes)
            or dataset in target_datasets
        }

        logger.info("Found %d datasets to scan", len(filtered_datasets))
        return filtered_datasets

    @staticmethod
    def table_name_contains_partition_date(table_name: str) -> bool:
        """Check if a table name contains a valid partition date."""
        pattern = r"(\d{4})(\d{2})(\d{2})"
        matches = re.findall(pattern, table_name)

        for match in matches:
            year, month, day = match
            try:
                datetime.datetime(int(year), int(month), int(day))
                return True
            except ValueError:
                continue
        return False

    def get_last_update_date(self, datasets_to_scan: Set[str]) -> pd.DataFrame:
        """Get the last update date for all tables in the specified datasets."""
        table_last_update_list = []
        for dataset in datasets_to_scan:
            query = f"""
                SELECT
                    table_id as table_name,
                    dataset_id as table_schema,
                    TIMESTAMP_MILLIS(last_modified_time) as last_modified_time
                FROM {dataset}.__TABLES__
            """
            table_last_update_list.append(pd.read_gbq(query))

        df = pd.concat(table_last_update_list, axis=0).reset_index(drop=True)
        logger.info("Retrieved last update dates for %d tables", len(df))
        return df

    def get_table_schedule(self) -> pd.DataFrame:
        """Get the schedule information for all tables."""
        query = f"""
            SELECT
                table_schema,
                table_name,
                REGEXP_EXTRACT(option_value, r'"schedule",\s*"([^"]+)"') as schedule_tag
            FROM
                `{self.config.project_id}`.`region-europe-west1`.INFORMATION_SCHEMA.TABLE_OPTIONS
            WHERE option_name = 'labels';
        """
        df = pd.read_gbq(query)
        logger.info("Retrieved schedule information for %d tables", len(df))
        return df

    def get_warning_tables(self) -> List[TableInfo]:
        """Get a list of tables that don't meet their expected update schedule."""
        datasets_to_scan = self.get_datasets_to_scan()
        table_last_update_df = self.get_last_update_date(datasets_to_scan)
        table_schedule_df = self.get_table_schedule()

        df = table_last_update_df.merge(
            table_schedule_df, on=["table_schema", "table_name"], how="left"
        ).assign(
            schedule_tag=lambda _df: _df["schedule_tag"].fillna(ScheduleTag.DEFAULT),
            last_modified_time=lambda _df: pd.to_datetime(
                _df["last_modified_time"]
            ).dt.tz_localize(None),
            is_partition_table=lambda _df: _df["table_name"].apply(
                self.table_name_contains_partition_date
            ),
        )

        warning_tables = df[
            df["last_modified_time"] < df["schedule_tag"].map(self._schedule_mapping)
        ]
        warning_tables = warning_tables[~warning_tables["is_partition_table"]]

        tables_to_archive = []
        tables_to_alert = []

        for _, row in warning_tables.iterrows():
            table = TableInfo(
                schema=row["table_schema"],
                name=row["table_name"],
                last_modified_time=row["last_modified_time"],
                schedule_tag=ScheduleTag(row["schedule_tag"]),
                is_partition_table=row["is_partition_table"],
            )

            if table.days_since_update >= self.config.archive_days_threshold:
                tables_to_archive.append(table)
            else:
                tables_to_alert.append(table)

        logger.info(
            "Found %d tables to archive and %d tables to alert",
            len(tables_to_archive),
            len(tables_to_alert),
        )

        # Archive tables that have exceeded the threshold
        for table in tables_to_archive:
            self._archive_table(table)

        return tables_to_alert + tables_to_archive

    def send_slack_alert(self, warning_tables: List[TableInfo]) -> None:
        """Send a Slack alert about tables that don't meet their expected update schedule."""
        if not self.config.webhook_url:
            logger.warning("No webhook URL provided, skipping Slack alert")
            return

        try:
            slack_msg = format_slack_message(
                warning_tables,
                self.config.env_emoji,
                self.config.archive_days_threshold,
                self.config.delete_instead_of_archive,
            )
            response = requests.post(
                self.config.webhook_url,
                json={"text": slack_msg},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(
                "Successfully sent Slack alert for %d tables", len(warning_tables)
            )
        except Exception as e:
            logger.error("Failed to send Slack alert: %s", str(e))
            raise
