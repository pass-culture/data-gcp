from collections.abc import Iterator
from dataclasses import dataclass

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

from src.constants import (
    ADDRESS_CITY_COL,
    ADDRESS_LATITUDE_COL,
    ADDRESS_LONGITUDE_COL,
    ADDRESS_STREET_COL,
    GCP_PROJECT_ID,
    OFFERER_ADDRESS_ID_COL,
    OFFERER_ADDRESS_LABEL_COL,
    OFFERER_ADDRESS_TABLE,
    POI_ADDRESS_COL,
    POI_COMMUNE_COL,
    POI_CSV_ID_COL,
    POI_ID_COL,
    POI_LATITUDE_COL,
    POI_LONGITUDE_COL,
    POI_NAME_COL,
    POI_POSTAL_CODE_COL,
    POI_TABLE,
    VENUE_ID_FK_COL,
)


def get_bigquery_client() -> bigquery.Client:
    """Return a BigQuery client scoped to the configured GCP project."""
    return bigquery.Client(project=GCP_PROJECT_ID)


@dataclass(frozen=True)
class BigQuerySource:
    """Declarative config for reading one BigQuery table into a DuckDB-ready DataFrame."""

    table: str
    columns: list[str]
    order_by: str
    not_null_columns: tuple[str, ...] = ()
    float_cast_columns: tuple[str, ...] = ()  # BQ NUMERIC -> float, needed by DuckDB's H3 functions

    def _build_query(self, limit: int | None = None, offset: int | None = None) -> str:
        where_sql = ""
        if self.not_null_columns:
            conditions = " AND ".join(f"{col} IS NOT NULL" for col in self.not_null_columns)
            where_sql = f"WHERE {conditions}"
        limit_sql = f"LIMIT {limit} OFFSET {offset}" if limit is not None else ""
        return f"""
            SELECT {", ".join(self.columns)}
            FROM `{GCP_PROJECT_ID}.{self.table}`
            {where_sql}
            ORDER BY {self.order_by}
            {limit_sql}
        """

    def _cast(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.float_cast_columns:
            return df
        return df.astype({col: float for col in self.float_cast_columns})

    def fetch(self) -> pd.DataFrame:
        """Fetch the whole table in one query. For small tables (e.g. POI, ~36k rows)."""
        return self._cast(pandas_gbq.read_gbq(self._build_query(), project_id=GCP_PROJECT_ID))

    def fetch_batches(self, batch_size: int = 5000) -> Iterator[pd.DataFrame]:
        """Page through the table in fixed-size batches. For large tables (e.g. offerer_address)."""
        offset = 0
        while True:
            query = self._build_query(limit=batch_size, offset=offset)
            batch_df = self._cast(pandas_gbq.read_gbq(query, project_id=GCP_PROJECT_ID))
            if batch_df.empty:
                return
            yield batch_df
            if len(batch_df) < batch_size:
                return
            offset += batch_size

POI_SOURCE = BigQuerySource(
    table=POI_TABLE,
    columns=[
        f"{POI_CSV_ID_COL} AS {POI_ID_COL}",
        POI_NAME_COL,
        POI_ADDRESS_COL,
        POI_POSTAL_CODE_COL,
        POI_COMMUNE_COL,
        POI_LATITUDE_COL,
        POI_LONGITUDE_COL,
    ],
    order_by=POI_ID_COL,
    not_null_columns=(POI_LATITUDE_COL, POI_LONGITUDE_COL),
    float_cast_columns=(POI_LATITUDE_COL, POI_LONGITUDE_COL),
)

OFFERER_ADDRESS_SOURCE = BigQuerySource(
    table=OFFERER_ADDRESS_TABLE,
    columns=[
        OFFERER_ADDRESS_ID_COL,
        OFFERER_ADDRESS_LABEL_COL,
        VENUE_ID_FK_COL,
        ADDRESS_STREET_COL,
        ADDRESS_CITY_COL,
        ADDRESS_LATITUDE_COL,
        ADDRESS_LONGITUDE_COL,
    ],
    order_by=OFFERER_ADDRESS_ID_COL,
    not_null_columns=(ADDRESS_LATITUDE_COL, ADDRESS_LONGITUDE_COL),
    float_cast_columns=(ADDRESS_LATITUDE_COL, ADDRESS_LONGITUDE_COL),
)


def iter_offerer_address_batches(batch_size: int = 5000) -> Iterator[pd.DataFrame]:
    """Iterate over applicative offerer_address rows in fixed-size batches."""
    return OFFERER_ADDRESS_SOURCE.fetch_batches(batch_size=batch_size)


def fetch_poi() -> pd.DataFrame:
    """Fetch POI rows from BigQuery."""
    return POI_SOURCE.fetch()
