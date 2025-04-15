from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from utils.constant import ENV_SHORT_NAME

CLOUD_SQL_TABLES_CONFIG: Dict[str, Dict] = {
    "past_offer_context": {
        "columns": {
            "id": "bigint",
            "call_id": "character varying",
            "context": "jsonb",
            "context_extra_data": "jsonb",
            "date": "timestamp without time zone",
            "user_id": "character varying",
            "user_bookings_count": "integer",
            "user_clicks_count": "integer",
            "user_favorites_count": "integer",
            "user_deposit_remaining_credit": "numeric",
            "user_iris_id": "character varying",
            "user_is_geolocated": "boolean",
            "user_extra_data": "jsonb",
            "offer_user_distance": "numeric",
            "offer_is_geolocated": "boolean",
            "offer_id": "character varying",
            "offer_item_id": "character varying",
            "offer_booking_number": "integer",
            "offer_stock_price": "numeric",
            "offer_creation_date": "timestamp without time zone",
            "offer_stock_beginning_date": "timestamp without time zone",
            "offer_category": "character varying",
            "offer_subcategory_id": "character varying",
            "offer_item_rank": "integer",
            "offer_item_score": "numeric",
            "offer_order": "integer",
            "offer_venue_id": "character varying",
            "offer_extra_data": "jsonb",
        },
        "time_column": "date",
        "partition_field": "import_date",
        "sql_table_name": "past_offer_context",
        "bigquery_table_name": "past_offer_context",
        "bigquery_dataset_name": f"raw_{ENV_SHORT_NAME}",
        "bigquery_schema": [
            {"name": "id", "type": "INTEGER"},
            {"name": "call_id", "type": "STRING"},
            {"name": "context", "type": "STRING"},
            {"name": "context_extra_data", "type": "STRING"},
            {"name": "date", "type": "TIMESTAMP"},
            {"name": "user_id", "type": "STRING"},
            {"name": "user_bookings_count", "type": "FLOAT"},
            {"name": "user_clicks_count", "type": "FLOAT"},
            {"name": "user_favorites_count", "type": "FLOAT"},
            {"name": "user_deposit_remaining_credit", "type": "FLOAT"},
            {"name": "user_iris_id", "type": "STRING"},
            {"name": "user_is_geolocated", "type": "BOOLEAN"},
            {"name": "user_extra_data", "type": "STRING"},
            {"name": "offer_user_distance", "type": "FLOAT"},
            {"name": "offer_is_geolocated", "type": "BOOLEAN"},
            {"name": "offer_id", "type": "STRING"},
            {"name": "offer_item_id", "type": "STRING"},
            {"name": "offer_booking_number", "type": "FLOAT"},
            {"name": "offer_stock_price", "type": "FLOAT"},
            {"name": "offer_creation_date", "type": "DATETIME"},
            {"name": "offer_stock_beginning_date", "type": "DATETIME"},
            {"name": "offer_category", "type": "STRING"},
            {"name": "offer_subcategory_id", "type": "STRING"},
            {"name": "offer_item_rank", "type": "FLOAT"},
            {"name": "offer_item_score", "type": "FLOAT"},
            {"name": "offer_order", "type": "FLOAT"},
            {"name": "offer_venue_id", "type": "STRING"},
            {"name": "offer_extra_data", "type": "STRING"},
            {"name": "import_date", "type": "DATE"},
        ],
    }
}


class MaterializedView(Enum):
    ENRICHED_USER = "enriched_user_mv"
    ITEM_IDS = "item_ids_mv"
    NON_RECOMMENDABLE_ITEMS = "non_recommendable_items_mv"
    IRIS_FRANCE = "iris_france_mv"
    RECOMMENDABLE_OFFERS = "recommendable_offers_raw_mv"


class SQLTableConfig:
    """Configuration for SQL table export."""

    def __init__(
        self,
        sql_table_name: str,
        bigquery_table_name: str,
        bigquery_dataset_name: str,
        columns: Dict[str, str],
        time_column: str,
        partition_field: str,
        bigquery_schema: List[Dict[str, str]],
    ):
        self.sql_table_name = sql_table_name
        self.bigquery_table_name = bigquery_table_name
        self.bigquery_dataset_name = bigquery_dataset_name
        self.columns = columns
        self.time_column = time_column
        self.partition_field = partition_field
        self.bigquery_schema = bigquery_schema

    def _get_time_conditions(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> str:
        """Helper to build WHERE clause conditions based on time range."""
        conditions = []
        if start_time:
            conditions.append(f"{self.time_column} >= '{start_time.isoformat()}'")
        if end_time:
            conditions.append(f"{self.time_column} <= '{end_time.isoformat()}'")
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    def _build_select_query(
        self,
        where_clause: str,
        execution_date: Optional[datetime] = None,
        database_name: Optional[str] = None,
    ) -> str:
        """Build the SELECT query with the given WHERE clause."""
        columns = ", ".join(self.columns.keys())
        query = f"SELECT {columns}"
        if execution_date:
            query += f", '{execution_date.strftime('%Y-%m-%d')}'::date as {self.partition_field}"
        query += f" FROM {database_name}.{self.sql_table_name}"
        if where_clause:
            query += f" {where_clause}"
        return query

    def get_extract_query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        execution_date: Optional[datetime] = None,
        database_name: Optional[str] = None,
    ) -> str:
        """Generate query to extract data between two timestamps for recovery."""
        where_clause = self._get_time_conditions(start_time, end_time)
        return self._build_select_query(where_clause, execution_date, database_name)

    def _build_delete_query(self, where_clause: str) -> str:
        return f"""
            DELETE FROM {self.sql_table_name}
            {where_clause}
        """

    def get_drop_table_query(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> str:
        """Generate query to delete data between two timestamps or all data."""
        where_clause = self._get_time_conditions(start_time, end_time)
        return self._build_delete_query(where_clause)


EXPORT_TABLES = {
    name: SQLTableConfig(**{**config})
    for name, config in CLOUD_SQL_TABLES_CONFIG.items()
}
