import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
from dateutil.relativedelta import relativedelta
from duckdb import DuckDBPyConnection

from utils.duckdb_utils import aggregate_kpi_data, query_monthly_kpi, query_yearly_kpi

logger = logging.getLogger(__name__)


class DataService:
    """Handles KPI data retrieval and aggregation from DuckDB."""

    def __init__(self, conn: DuckDBPyConnection):
        self.conn = conn

    def get_kpi_data(
        self,
        kpi_name: str,
        dimension_name: str,
        dimension_value: str,
        ds: str,
        scope: str,
        table_name: str,
        select_field: str = "kpi",
        agg_type: str = "sum",
    ) -> Optional[Dict[str, Dict[str, float]]]:
        """
        Get complete KPI data (yearly and monthly aggregations).

        Args:
            kpi_name: KPI identifier
            dimension_name: NAT, REG, ACAD, DEP
            dimension_value: Specific value for dimension
            ds: Consolidation date in YYYY-MM-DD format
            scope: individual or collective
            table_name: DuckDB table name
            select_field: Field to select (kpi, numerator, denominator)
            agg_type: Aggregation type (sum, max, december, august, avg ,wavg)

        Returns:
            Dict with yearly and monthly aggregated data, None if failed
        """
        try:
            # Get yearly data
            yearly_data = self._get_yearly_kpi_data(
                kpi_name, dimension_name, dimension_value, ds, scope, table_name
            )
            yearly_agg = self._aggregate_yearly_data(
                yearly_data, agg_type, select_field, scope
            )

            # Get monthly data
            monthly_data = self._get_monthly_kpi_data(
                kpi_name, dimension_name, dimension_value, ds, table_name
            )
            # monthly_agg = self._aggregate_monthly_data(monthly_data, agg_type, select_field, scope)
            monthly_formatted = self._format_monthly_data(
                monthly_data, select_field, ds
            )

            return {"yearly": yearly_agg, "monthly": monthly_formatted}

        except Exception as e:
            logger.warning(f"Unexpected error retrieving KPI '{kpi_name}': {e}")
            return None

    def _get_yearly_kpi_data(
        self,
        kpi_name: str,
        dimension_name: str,
        dimension_value: str,
        ds: str,
        scope: str,
        table_name: str,
        start_year: int = 2021,
    ) -> pd.DataFrame:
        """Get yearly KPI data using existing duckdb_utils function."""
        from datetime import datetime

        # Determine end year from ds
        ds_date = datetime.strptime(ds, "%Y-%m-%d")
        end_year = ds_date.year

        # For collective scope, adjust for scholar year
        if scope == "collective" and ds_date.month < 9:
            end_year -= 1

        return query_yearly_kpi(
            conn=self.conn,
            ds=ds,
            table_name=table_name,
            kpi_name=kpi_name,
            dimension_name=dimension_name,
            dimension_value=dimension_value,
            start_year=start_year,
            end_year=end_year,
            scope=scope,
        )

    def _get_monthly_kpi_data(
        self,
        kpi_name: str,
        dimension_name: str,
        dimension_value: str,
        ds: str,
        table_name: str,
    ) -> pd.DataFrame:
        """Get monthly KPI data using existing duckdb_utils function."""
        return query_monthly_kpi(
            conn=self.conn,
            table_name=table_name,
            kpi_name=kpi_name,
            dimension_name=dimension_name,
            dimension_value=dimension_value,
            ds=ds,
        )

    def _aggregate_yearly_data(
        self, data: pd.DataFrame, agg_type: str, select_field: str, scope: str
    ) -> Dict[Union[int, str], float]:
        """Aggregate yearly data using existing duckdb_utils function."""
        return aggregate_kpi_data(
            data=data,
            agg_type=agg_type,
            time_grouping="yearly",
            select_field=select_field,
            scope=scope,
        )

    def _aggregate_monthly_data(
        self, data: pd.DataFrame, agg_type: str, select_field: str, scope: str
    ) -> Dict[Union[str, pd.Timestamp], float]:
        """Aggregate monthly data using existing duckdb_utils function."""
        return aggregate_kpi_data(
            data=data,
            agg_type=agg_type,
            time_grouping="monthly",
            select_field=select_field,
            scope=scope,
        )

    def _format_monthly_data(
        self, data: pd.DataFrame, select_field: str, ds: str
    ) -> Dict[str, float]:
        """
        Format monthly data into dict with MM/YYYY keys (no aggregation).
        Similar to yearly aggregation but just formatting.
        """
        if data.empty:
            return {}

        if select_field not in ["kpi", "numerator", "denominator"]:
            select_field = "kpi"

        data = data.copy()
        data["partition_month"] = pd.to_datetime(
            data["partition_month"], errors="coerce"
        )
        data = data.dropna(subset=["partition_month"])

        # Format to MM/YYYY and return as dict (no aggregation, just direct mapping)
        result = {}
        for _, row in data.iterrows():
            month_key = row["partition_month"].strftime("%m/%Y")
            value = row[select_field]
            if pd.notna(value):
                result[month_key] = float(value)

        return result

    def get_top_rankings(
        self,
        dimension_name: str,
        dimension_value: str,
        ds: str,
        table_name: str,
        top_n: int = 50,
        select_fields: List[str] = [],
        order_by: List[str] = [],
    ) -> Optional[pd.DataFrame]:
        """
        Get top N rankings for a given dimension.

        Args:
            dimension_name: NAT, REG, ACAD, DEP
            dimension_value: Specific value for dimension
            ds: Consolidation date in YYYY-MM-DD format
            table_name: DuckDB table name
            top_n: Number of top entries to retrieve
            order_by: List of fields to order by (descending)

        Returns:
            DataFrame with top rankings, None if failed
        """
        assert order_by, "order_by list cannot be empty"
        assert len(order_by) <= 2, "order_by can have at most 2 fields"
        assert select_fields, "select_fields list cannot be empty"
        full_select = select_fields + [
            field for field in order_by if field not in select_fields
        ]
        reorder_by = [order_by[0], "rank"] if len(order_by) > 1 else ["rank"]
        partition = f"PARTITION BY {order_by[0]}" if len(order_by) == 2 else ""

        ds_date = datetime.strptime(ds, "%Y-%m-%d").date()
        previous_month = ds_date - relativedelta(months=1)
        previous_month_str = previous_month.strftime("%Y-%m-%d")
        try:
            query = f"""
                SELECT {', '.join(select_fields)}, rank
                FROM (
                    SELECT {', '.join(full_select)},
                        ROw_NUMBER() OVER ({partition} ORDER BY {order_by[-1]} DESC) as rank
                    FROM {table_name}
                    WHERE dimension_name = ? AND dimension_value = ? AND partition_month = ?
                )
                WHERE rank <= ?
                ORDER BY {', '.join(reorder_by)} ASC
            """
            params = [dimension_name, dimension_value, previous_month_str, top_n]
            # print("DEBUG: Query with parameters substituted:")
            # manual_query = query.replace('?', '{}').format(*[f"'{p}'" if isinstance(p, str) else str(p) for p in params])
            # print(manual_query)
            result = self.conn.execute(query, params).df()
            # print(f"DEBUG: Query returned {len(result)} rows")

            return result
        except Exception as e:
            logger.warning(
                f"Failed to retrieve top rankings for {dimension_name}={dimension_value}: {e}"
            )
            return None
