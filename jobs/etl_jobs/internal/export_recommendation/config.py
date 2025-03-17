from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

from utils import ENV_SHORT_NAME


class DatasetType(Enum):
    ML_RECO = "ml_reco"
    SEED = "seed"


class MaterializedView(Enum):
    ENRICHED_USER = "enriched_user_mv"
    ITEM_IDS = "item_ids_mv"
    NON_RECOMMENDABLE_ITEMS = "non_recommendable_items_mv"
    IRIS_FRANCE = "iris_france_mv"
    RECOMMENDABLE_OFFERS = "recommendable_offers_raw_mv"


CONCURRENT_MATERIALIZED_VIEWS = [
    MaterializedView.ENRICHED_USER,
    MaterializedView.ITEM_IDS,
    MaterializedView.NON_RECOMMENDABLE_ITEMS,
    MaterializedView.IRIS_FRANCE,
]


@dataclass
class TableConfig:
    columns: Dict[str, str]
    bigquery_table_name: str
    dataset_type: DatasetType

    @property
    def dataset_id(self) -> str:
        """Get the dataset ID with environment suffix."""
        return f"{self.dataset_type.value}_{ENV_SHORT_NAME}"

    @property
    def column_definitions(self) -> str:
        """Generate PostgreSQL column definitions for table creation."""
        return ", ".join([f'"{col}" {dtype}' for col, dtype in self.columns.items()])

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())

    @property
    def select_columns(self) -> str:
        """Generate BigQuery select statement columns."""
        return ", ".join([f"`{col}`" for col in self.column_names])

    def get_export_query(self, project_id: str) -> str:
        """Generate BigQuery export query with proper column selection."""
        return f"""
            SELECT {self.select_columns}
            FROM `{project_id}.{self.dataset_id}.{self.bigquery_table_name}`
        """

    def get_create_table_sql(self, table_name: str) -> str:
        """Generate PostgreSQL create table statement."""
        return f"""
            CREATE TABLE IF NOT EXISTS public.{table_name} (
                {self.column_definitions}
            );
        """


# Tables configuration with complete schema
TABLES_CONFIG: Dict[str, Dict] = {
    "enriched_user": {
        "columns": {
            "user_id": "character varying",
            "user_deposit_creation_date": "timestamp without time zone",
            "user_birth_date": "timestamp without time zone",
            "user_deposit_initial_amount": "real",
            "user_theoretical_remaining_credit": "real",
            "booking_cnt": "integer",
            "consult_offer": "integer",
            "has_added_offer_to_favorites": "integer",
        },
        "bigquery_table_name": "user_statistics",
        "dataset_type": DatasetType.ML_RECO,
    },
    "recommendable_offers_raw": {
        "columns": {
            "item_id": "character varying",
            "offer_id": "character varying",
            "offer_creation_date": "timestamp without time zone",
            "stock_beginning_date": "timestamp without time zone",
            "booking_number": "integer",
            "venue_latitude": "decimal",
            "venue_longitude": "decimal",
            "default_max_distance": "integer",
            "unique_id": "character varying",
            "is_sensitive": "boolean",
            "is_geolocated": "boolean",
        },
        "bigquery_table_name": "recommendable_offer",
        "dataset_type": DatasetType.ML_RECO,
    },
    "non_recommendable_items_data": {
        "columns": {"user_id": "character varying", "item_id": "character varying"},
        "bigquery_table_name": "user_booked_item",
        "dataset_type": DatasetType.ML_RECO,
    },
    "iris_france": {
        "columns": {
            "id": "character varying",
            "irisCode": "character varying",
            "centroid": "geography",
            "shape": "geography",
        },
        "bigquery_table_name": "iris_france",
        "dataset_type": DatasetType.SEED,
    },
}

# CSV Export settings
CSV_EXPORT_CONFIG = {
    "compression": "GZIP",
    "field_delimiter": ",",
    "print_header": False,
    "quote": '"',
    "escape_char": '"',
}

# Cloud SQL import settings
CLOUD_SQL_IMPORT_CONFIG = {
    "fileType": "CSV",
    "csvImportOptions": {
        "quote": '"',
        "escape": '"',
        "fields_terminated_by": ",",
    },
}

# Initialize table configs
TABLES = {
    name: TableConfig(**{**config, "dataset_type": config["dataset_type"]})
    for name, config in TABLES_CONFIG.items()
}
