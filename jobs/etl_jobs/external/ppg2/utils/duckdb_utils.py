from typing import Dict, Union, Optional

from duckdb import DuckDBPyConnection
import typer
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
import pandas as pd


# --- Exceptions ---
class QueryError(Exception):
    pass

class AggregationError(Exception):
    pass

# --- Query helpers ---
def query_kpi_data(
    conn: DuckDBPyConnection,
    table_name: str,
    kpi_name: str,
    dimension_name: str,
    dimension_value: str = None,
    ds: str = None,
) -> pd.DataFrame:
    """Query KPI data from DuckDB for a KPI & dimension."""
    try:
        where = ["kpi_name = ?", "dimension_name = ?"]
        params = [kpi_name, dimension_name]

        if dimension_value and dimension_name != "NAT":
            where.append("dimension_value = ?")
            params.append(dimension_value)

        if ds:
            end_date = datetime.strptime(ds, "%Y-%m-%d").date()
            start_date = date(2021, 1, 1)
            where.append("partition_month >= ?")
            where.append("partition_month <= ?")
            params.extend([start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")])

        query = f"""
            SELECT partition_month, kpi_name, numerator, denominator, kpi
            FROM {table_name}
            WHERE {" AND ".join(where)}
            ORDER BY partition_month
        """

        return conn.execute(query, params).df()

    except Exception as e:
        raise QueryError(f"Failed to query KPI {kpi_name}: {e}")


def query_yearly_kpi(
    conn: DuckDBPyConnection,
    ds: str,
    table_name: str,
    kpi_name: str,
    dimension_name: str,
    dimension_value: str,
    start_year: int = 2021,
    end_year: Optional[int] = None,
    scope: str = "individual",
) -> pd.DataFrame:
    """
    Query yearly KPI data.
    - Individual: calendar year (Jan-Dec)
    - Collective: scholar year (Sep-Aug)
    """
    # end_date = datetime.strptime(ds, "%Y-%m-%d").date()
    if end_year is None:
        end_year = date.today().year 

    try:
        all_data = []
        for year in range(start_year, end_year + 1):
            if scope == "individual":
                start_date = date(year, 1, 1)
                end_date_ = date(year, 12, 31)
                year_label = year
            else:  # collective scholar year
                start_date = date(year, 9, 1)
                end_date_ = date(year + 1, 8, 31)
                year_label = f"{year}-{year+1}"

            # where = ["kpi_name = ?", "dimension_name = ?", "partition_month >= CAST(? as DATE)", "partition_month <=  CAST(? as DATE)","dimension_value = ?"]
            where = ["kpi_name = ?", "dimension_name = ?", "partition_month >= ?", "partition_month <=  ?","dimension_value = ?"]
            params = [kpi_name, dimension_name, start_date.strftime("%Y-%m-%d"), end_date_.strftime("%Y-%m-%d"),dimension_value]
            # where = ["kpi_name = ?", "dimension_name = ?","dimension_value = ?"]
            # params = [kpi_name, dimension_name,dimension_value]


            query = f"""
                SELECT partition_month, kpi_name, numerator, denominator, kpi
                FROM {table_name}
                WHERE {" AND ".join(where)}
                ORDER BY partition_month
            """
            df = conn.execute(query, params).df()
            df["year_label"] = year_label
            all_data.append(df)

        if all_data:
            typer.echo(f"Returning {len(pd.concat(all_data, ignore_index=True))} data")
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame(columns=["partition_month", "kpi_name", "numerator", "denominator", "kpi", "year_label"])

    except Exception as e:
        raise QueryError(f"Failed to query yearly KPI {kpi_name} for scope {scope}: {e}")



def query_monthly_kpi(
    conn: DuckDBPyConnection,
    table_name: str,
    kpi_name: str,
    dimension_name: str,
    dimension_value: str = None,
    ds: str = None,
) -> pd.DataFrame:
    if ds:
        ref_date = datetime.strptime(ds, "%Y-%m-%d").date()
    else:
        ref_date = date.today()
    last_month = ref_date.replace(day=1) - relativedelta(months=1)

    periods = [
        last_month - relativedelta(months=1),   # prev-2
        last_month,                             # prev-1
        last_month - relativedelta(months=12)   # prev-13
    ]

    where = ["kpi_name = ?", "dimension_name = ?","dimension_value = ?", "partition_month IN (?, ?, ?)"]
    params = [kpi_name, dimension_name,dimension_value] + [p.strftime("%Y-%m-%d") for p in periods]


    query = f"""
        SELECT partition_month, kpi_name, numerator, denominator, kpi
        FROM {table_name}
        WHERE {" AND ".join(where)}
        ORDER BY partition_month
    """
    try:
        return conn.execute(query, params).df()
    except Exception as e:
        raise QueryError(f"Failed to query monthly KPI {kpi_name}: {e}")


# --- Aggregation helpers ---
def aggregate_kpi_data(
    data: pd.DataFrame,
    agg_type: str = "sum",
    time_grouping: str = "yearly",
    select_field: str = "kpi",
    scope: str = "individual",
) -> Dict[Union[int, str], float]:
    """
    Aggregate KPI data:
    - 'individual': standard calendar year
    - 'collective': scholar year (Sep-Aug)
    """
    if data.empty:
        return {}

    data = data.copy()
    data["partition_month"] = pd.to_datetime(data["partition_month"], errors="coerce")
    data = data.dropna(subset=["partition_month"])

    if time_grouping == "yearly":
        group_col = "year_label"
    else:
        group_col = "partition_month"

    if select_field not in ["kpi", "numerator", "denominator"]:
        select_field = "kpi"

    if agg_type == "sum":
        grouped = data.groupby(group_col)[select_field].sum()
    elif agg_type == "max":
        grouped = data.groupby(group_col)[select_field].max()
    elif agg_type == "december":
        # For individual, Dec logic; for collective, last month of scholar year = Aug
        if scope == "individual":
            grouped = data[data["partition_month"].dt.month == 12].groupby(group_col)[select_field].first()
        else:
            grouped = data[data["partition_month"].dt.month == 8].groupby(group_col)[select_field].first()
    elif agg_type == "wavg" and select_field == "kpi":
        data["weighted_value"] = data["numerator"]
        grouped = (data.groupby(group_col)["weighted_value"].sum() / data.groupby(group_col)["denominator"].sum()).to_dict()
        return grouped
    else:
        grouped = data.groupby(group_col)[select_field].sum()

    return {k: float(v) for k, v in grouped.to_dict().items() if pd.notna(v)}