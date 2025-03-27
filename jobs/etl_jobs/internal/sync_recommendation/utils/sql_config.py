from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Optional


class MaterializedView(Enum):
    ENRICHED_USER = "enriched_user_mv"
    ITEM_IDS = "item_ids_mv"
    NON_RECOMMENDABLE_ITEMS = "non_recommendable_items_mv"
    IRIS_FRANCE = "iris_france_mv"
    RECOMMENDABLE_OFFERS = "recommendable_offers_raw_mv"


@dataclass
class SQLTableConfig:
    """Configuration for cloudSQL table retrieval."""

    sql_table_name: str
    bigquery_table_name: str
    bigquery_dataset_name: str
    columns: Dict[str, str]
    time_column: str
    partition_field: str

    @classmethod
    def _get_time_conditions(
        cls, start_time: Optional[datetime], end_time: Optional[datetime]
    ) -> str:
        """Helper to build WHERE clause conditions based on time range."""
        conditions = []
        if start_time:
            conditions.append(f"{cls.time_column} >= '{start_time.isoformat()}'")
        if end_time:
            conditions.append(f"{cls.time_column} <= '{end_time.isoformat()}'")
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""

    @classmethod
    def _build_select_query(cls, where_clause: str, execution_date: datetime) -> str:
        columns_str = ", ".join(cls.columns.keys())
        return f"""
            SELECT
                {columns_str},
                DATE({execution_date}) AS {cls.partition_field}
            FROM {cls.sql_table_name}
            {where_clause}
        """

    @classmethod
    def _build_delete_query(cls, where_clause: str) -> str:
        return f"""
            DELETE FROM {cls.sql_table_name}
            {where_clause}
        """

    @classmethod
    def get_extract_query(
        cls,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        execution_date: datetime = None,
    ) -> str:
        """Generate query to extract data between two timestamps for recovery."""
        where_clause = cls._get_time_conditions(start_time, end_time)
        return cls._build_select_query(where_clause, execution_date)

    @classmethod
    def get_drop_table_query(
        cls, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> str:
        """Generate query to delete data between two timestamps or all data."""
        where_clause = cls._get_time_conditions(start_time, end_time)
        return cls._build_delete_query(where_clause)


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
    }
}


EXPORT_TABLES = {
    name: SQLTableConfig(**{**config})
    for name, config in CLOUD_SQL_TABLES_CONFIG.items()
}
