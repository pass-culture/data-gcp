from dataclasses import dataclass, field
from typing import Dict, List, Optional

from common.config import ENV_SHORT_NAME

BQ_EXPORT_SCHEMA = f"export_{ENV_SHORT_NAME}"
DBT_MODELS_PREFIX = "exp_clickhouse"
BQ_CLICKHOUSE_TABLE_PREFIX = "clickhouse"
CLICKHOUSE_STAGING_DATASET = "intermediate"


@dataclass
class TableConfig:
    model_name: str
    mode: str
    partition_key: Optional[str]

    @property
    def dbt_model(self) -> str:
        return f"{DBT_MODELS_PREFIX}__{self.model_name}"

    @property
    def bigquery_dataset_name(self) -> str:
        return f"{BQ_EXPORT_SCHEMA}"

    @property
    def bigquery_table_name(self) -> str:
        return f"{BQ_CLICKHOUSE_TABLE_PREFIX}_{self.model_name}"

    @property
    def clickhouse_dataset_name(self) -> str:
        return CLICKHOUSE_STAGING_DATASET

    def to_dict(self) -> Dict:
        return {
            "dbt_model": self.dbt_model,
            "bigquery_dataset_name": self.bigquery_dataset_name,
            "bigquery_table_name": self.bigquery_table_name,
            "clickhouse_table_name": self.model_name,
            "clickhouse_dataset_name": self.clickhouse_dataset_name,
            "mode": self.mode,
            "partition_key": self.partition_key,
        }


@dataclass
class AnalyticsConfig:
    clickhouse_dataset_name: str
    clickhouse_table_name: str
    depends_list: List[str] = field(default_factory=list)
    ch_session_settings: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "clickhouse_dataset_name": self.clickhouse_dataset_name,
            "clickhouse_table_name": self.clickhouse_table_name,
            "depends_list": self.depends_list,
            "ch_session_settings": self.ch_session_settings,
        }


# List of models to be exported
CLICKHOUSE_LOADING_CONFIGS = [
    TableConfig(model_name="booking", mode="overwrite", partition_key="update_date"),
    TableConfig(
        model_name="collective_booking", mode="overwrite", partition_key="update_date"
    ),
    TableConfig(
        model_name="native_event", mode="incremental", partition_key="partition_date"
    ),
    TableConfig(
        model_name="venue_offer_statistic", mode="overwrite", partition_key=None
    ),
]

# List of aggregated tables to be refreshed
CLICKHOUSE_ANALYTICS_TRANSFORMATION_CONFIGS = [
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="offer_consultation_cumulative",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="daily_aggregated_venue_offer_consultation",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="last_30_day_venue_top_offer_consultation",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="monthly_aggregated_venue_collective_revenue",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="monthly_aggregated_venue_individual_revenue",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="monthly_aggregated_venue_revenue",
        depends_list=[
            "monthly_aggregated_venue_collective_revenue",
            "monthly_aggregated_venue_individual_revenue",
        ],
        ch_session_settings={"max_partitions_per_insert_block": 200},
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="yearly_aggregated_venue_collective_revenue",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="yearly_aggregated_venue_individual_revenue",
    ),
    AnalyticsConfig(
        clickhouse_dataset_name="analytics",
        clickhouse_table_name="yearly_aggregated_venue_revenue",
        depends_list=[
            "yearly_aggregated_venue_collective_revenue",
            "yearly_aggregated_venue_individual_revenue",
        ],
    ),
]
