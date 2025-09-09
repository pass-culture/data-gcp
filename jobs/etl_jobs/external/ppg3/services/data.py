from typing import Dict, List, Optional, Union
from duckdb import DuckDBPyConnection
import typer
import logging
import pandas as pd

from utils.duckdb_utils import query_yearly_kpi, query_monthly_kpi, aggregate_kpi_data

logger = logging.getLogger(__name__)


class KPIDataService:
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
        agg_type: str = "sum"
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
            yearly_agg = self._aggregate_yearly_data(yearly_data, agg_type, select_field, scope)
            
            # Get monthly data
            monthly_data = self._get_monthly_kpi_data(
                kpi_name, dimension_name, dimension_value, ds, table_name
            )
            # monthly_agg = self._aggregate_monthly_data(monthly_data, agg_type, select_field, scope)
            monthly_formatted = self._format_monthly_data(monthly_data, select_field, ds)
            
            return {
                "yearly": yearly_agg,
                "monthly": monthly_formatted
            }
            

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
        start_year: int = 2021
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
            scope=scope
        )
    
    def _get_monthly_kpi_data(
        self,
        kpi_name: str,
        dimension_name: str,
        dimension_value: str, 
        ds: str,
        table_name: str
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
        self,
        data: pd.DataFrame,
        agg_type: str,
        select_field: str,
        scope: str
    ) -> Dict[Union[int, str], float]:
        """Aggregate yearly data using existing duckdb_utils function."""
        return aggregate_kpi_data(
            data=data,
            agg_type=agg_type,
            time_grouping="yearly",
            select_field=select_field,
            scope=scope
        )
    
    def _aggregate_monthly_data(
        self,
        data: pd.DataFrame,
        agg_type: str,
        select_field: str,
        scope: str
    ) -> Dict[Union[str, pd.Timestamp], float]:
        """Aggregate monthly data using existing duckdb_utils function.""" 
        return aggregate_kpi_data(
            data=data,
            agg_type=agg_type,
            time_grouping="monthly",
            select_field=select_field,
            scope=scope
        )
    def _format_monthly_data(
        self,
        data: pd.DataFrame,
        select_field: str,
        ds: str
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
        data["partition_month"] = pd.to_datetime(data["partition_month"], errors="coerce")
        data = data.dropna(subset=["partition_month"])
        
        # Format to MM/YYYY and return as dict (no aggregation, just direct mapping)
        result = {}
        for _, row in data.iterrows():
            month_key = row["partition_month"].strftime("%m/%Y")
            value = row[select_field]
            if pd.notna(value):
                result[month_key] = float(value)
        
        return result