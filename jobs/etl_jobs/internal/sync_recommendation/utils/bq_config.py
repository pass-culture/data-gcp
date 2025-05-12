from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

from utils.constant import ENV_SHORT_NAME


class DatasetType(Enum):
    ML_RECO = "ml_reco"
    SEED = "seed"


BQ_TABLES_CONFIG: Dict[str, Dict] = {
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
            "new_user_bookings_count": "integer",
            "new_user_clicks_count": "integer",
            "new_user_favorites_count": "integer",
            "new_user_deposit_amount": "real",
            "new_user_amount_spent": "real",
        },
        "bigquery_table_name": "user_statistics",
        "cloud_sql_table_name": "enriched_user",
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
            "new_offer_is_geolocated": "boolean",
            "new_offer_creation_days": "integer",
            "new_offer_stock_price": "decimal",
            "new_offer_stock_beginning_days": "decimal",
            "new_offer_centroid_x": "decimal",
            "new_offer_centroid_y": "decimal",
        },
        "bigquery_table_name": "recommendable_offer",
        "cloud_sql_table_name": "recommendable_offers_raw",
        "dataset_type": DatasetType.ML_RECO,
    },
    "non_recommendable_items_data": {
        "columns": {"user_id": "character varying", "item_id": "character varying"},
        "bigquery_table_name": "user_booked_item",
        "cloud_sql_table_name": "non_recommendable_items_data",
        "dataset_type": DatasetType.ML_RECO,
    },
    "iris_france": {
        "columns": {
            "id": "character varying",
            "irisCode": "character varying",
            "centroid": "geometry",
            "shape": "geometry",
        },
        "bigquery_table_name": "iris_france",
        "cloud_sql_table_name": "iris_france",
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


@dataclass
class BQTableConfig:
    columns: Dict[str, str]
    bigquery_table_name: str
    cloud_sql_table_name: str
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

    @property
    def duckdb_select_columns(self) -> str:
        """Generate DuckDB select statement with proper column conversions for geography types."""
        select_cols = []
        for col_name, col_type in self.columns.items():
            if col_type.lower() == "geography":
                select_cols.append(f"ST_AsText({col_name}) as {col_name}")
            else:
                select_cols.append(col_name)
        return ", ".join(select_cols)

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


# Initialize table configs
EXPORT_TABLES = {
    name: BQTableConfig(**{**config, "dataset_type": config["dataset_type"]})
    for name, config in BQ_TABLES_CONFIG.items()
}
