from typing import Dict, List, Optional, Union, Iterable
from pathlib import Path
import logging
import warnings
import typer
from google.cloud import bigquery
import duckdb
import pandas as pd

from config import (
    GCP_PROJECT,
    BIGQUERY_ANALYTICS_DATASET,
    REGION_HIERARCHY_TABLE,
)
from xlsx_utils.core import BASE_TEMPLATE, SOURCE_TABLES, REPORTS, Stakeholder, StakeholderType, SheetType, Report


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def build_region_hierarchy(
    project_id: str = GCP_PROJECT,
    dataset: str = BIGQUERY_ANALYTICS_DATASET,
    table: str = REGION_HIERARCHY_TABLE,
) -> Dict[str, Dict]:
    """
    Build a nested dict with enhanced academy-department mapping from BigQuery.

    Args:
        project_id: GCP project ID
        dataset: BigQuery dataset
        table: BigQuery table name

    Returns:
        Dict with structure:
        {
            region_name: {
                "departements": [dept1, dept2, ...],
                "academies": [acad1, acad2, ...],
                "academy_departments": {
                    academy_name: [dept1, dept2, ...]
                }
            }
        }

    Raises:
        HierarchyError: If hierarchy building fails
    """

    try:
        client = bigquery.Client(project=project_id)

        query = f"""
        SELECT
            region_name,
            dep_name,
            academy_name
        FROM `{project_id}.{dataset}.{table}`
        WHERE region_name IS NOT NULL
        ORDER BY region_name, academy_name, dep_name
        """

        typer.echo(f"🌍 Querying region hierarchy: {project_id}.{dataset}.{table}")

        rows = client.query(query).result()
        row_count = 0
        by_region = {}

        for row in rows:
            row_count += 1
            region = row.region_name
            dep = row.dep_name
            acad = row.academy_name

            if region not in by_region:
                by_region[region] = {
                    "departements": set(),
                    "academies": set(),
                    "academy_departments": {},
                }

            bucket = by_region[region]

            # Add department if present
            if dep:
                bucket["departements"].add(dep)

            # Add academy if present
            if acad:
                bucket["academies"].add(acad)

            # Map departments to academies
            if acad and dep:
                if acad not in bucket["academy_departments"]:
                    bucket["academy_departments"][acad] = set()
                bucket["academy_departments"][acad].add(dep)

        # Convert sets to sorted lists for consistent output
        sorted_by_region = {}
        for region, scope in sorted(by_region.items()):
            sorted_by_region[region] = {
                "departements": sorted(scope["departements"]),
                "academies": sorted(scope["academies"]),
                "academy_departments": {
                    academy: sorted(departments)
                    for academy, departments in scope["academy_departments"].items()
                },
            }

        # Log summary statistics
        total_regions = len(sorted_by_region)
        total_academies = sum(len(r["academies"]) for r in sorted_by_region.values())
        total_departments = sum(
            len(r["departements"]) for r in sorted_by_region.values()
        )

        typer.echo(
            f"✅ Loaded hierarchy: {total_regions} regions, {total_academies} academies, {total_departments} departments"
        )
        typer.echo(f"   📊 Total records processed: {row_count:,}")

        return sorted_by_region
    except Exception as e:
        typer.secho(f"❌ Failed to build region hierarchy: {e}", fg="red")
        raise

def get_available_regions():
    return sorted(build_region_hierarchy().keys())


def sanitize_date_fields(df: pd.DataFrame, fields: Union[str, Iterable[str]]) -> pd.DataFrame:
    """Ensure date fields are in YYYY-MM-DD format."""
    if isinstance(fields, str):
        fields = [fields]

    out = df.copy()

    for col in fields:
        if col not in out.columns:
            logger.warning("sanitize_date_field: column '%s' not found; skipping.", col)
            continue
        try:
            s = out[col]
            # if BigQuery db-dtypes 'dbdate', turn into plain object first
            if getattr(s.dtype, "name", "") == "dbdate":
                s = s.astype("object")
            out[col] = pd.to_datetime(s, errors="coerce").dt.date
            s = out[col]
        except Exception as e:
            logger.warning("sanitize_date_fields: failed to convert '%s' to date: %s", col, e)

    return out

def sanitize_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert problematic numeric columns to appropriate types."""
    df = df.copy()
    # Convert object columns that should be numeric to float64
    for col in df.columns:
        if ('amount' in col.lower() or 'quantity' in col.lower()) or col.lower() in ['numerator', 'denominator', 'kpi']:
            if df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    return df


class ExportSession:
    """Manages the entire export session with proper resource cleanup."""
    def __init__(self, ds: str):#, template_path: Path, output_dir: Path, scope: str, target: str):
        self.ds = ds
        # self.template_path = template_path
        # self.output_dir = output_dir
        # self.scope: str = scope
        # self.target = target
        self.tables_to_load = SOURCE_TABLES
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        # self.hierarchy_data = None
        
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    
    def load_data(self):
        """Load tables based on self.scope."""
        
        client = bigquery.Client(project=GCP_PROJECT)
        self.conn = duckdb.connect(":memory:")
        
        for _, config in self.tables_to_load.items():
            self._load_table(client,config)
    
    def _load_table(self, client: bigquery.Client, config: Dict):
        """Load a single table into DuckDB."""
        table_name = config['table']
        dataset = config.get('dataset', BIGQUERY_ANALYTICS_DATASET)
        project_id = config.get('project', GCP_PROJECT)
        typer.echo(f"Loading {table_name} from BigQuery...")
        query = f"SELECT * FROM `{project_id}.{dataset}.{table_name}` WHERE partition_month = '{self.ds}'"
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*BigQuery Storage module not found.*")
            df = client.query(query).to_dataframe()
            typer.echo(f"{table_name} dtypes: {df.dtypes}")
        if df.empty:
            typer.echo(f"Warning: {table_name} returned no data")
            return
        
        df = sanitize_date_fields(df, "partition_month")
        df = sanitize_numeric_types(df)
            
        typer.echo(f"Loaded {len(df):,} records for {table_name}")
        
        # Register and create table
        self.conn.register(f"{table_name}_tmp", df)
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}_tmp")
        
        # Create indexes based on available columns
        self._create_indexes(table_name, df.columns.tolist())
        typer.echo(f"MOCK -> Created {table_name} table with indexes")

    
    def _create_indexes(self, table_name: str, columns: List[str]):
        """Create indexes on commonly queried columns."""
        pass
    
    def process_stakeholder(self, stakeholder_type: str, name:str, output_path: Path, ds: str):
        """Process reports for a given stakeholder."""
        stakeholder = Stakeholder(
            type=StakeholderType.MINISTERE if stakeholder_type == "ministere" else StakeholderType.DRAC,
            name="Ministère" if stakeholder_type == "ministere" else name,
        )
        typer.secho(f"➡️ Processing reports for {stakeholder.type.value} - {stakeholder.name}", fg="cyan")
        
        
        # Generate reports based on stakeholder's desired reports
        for report_key in stakeholder.desired_reports:
            if report_key not in REPORTS:
                typer.secho(f"❌ Unknown report key: {report_key}", fg="red")
                raise typer.Exit(code=1)
            
            filename = f"{report_key}"
            typer.secho(f"➡️ Creating report {filename}.xlsx", fg="cyan")
            # Copy base template and select and rename deisred sheets
            report = Report(
                stakeholder=stakeholder,
                base_template_path=BASE_TEMPLATE,
                output_path=output_path / f"{filename}.xlsx"
            )        
        
        pass
    
    
    
    
    def process_stakeholder_old(self, stakeholder_type: str, name:str, output_path: Path, ds: str):
        """Process reports for a given stakeholder."""
        stakeholder = Stakeholder(
            type=StakeholderType.MINISTERE if stakeholder_type == "ministere" else StakeholderType.DRAC,
            name="Ministère" if stakeholder_type == "ministere" else name,
        )
        typer.secho(f"➡️ Processing reports for {stakeholder.type.value} - {stakeholder.name}", fg="cyan")
        # Generate reports based on stakeholder's desired reports
        for report_key in stakeholder.desired_reports:
            if report_key not in REPORTS:
                typer.secho(f"❌ Unknown report key: {report_key}", fg="red")
                raise typer.Exit(code=1)
            
            filename = f"{report_key}"
            typer.secho(f"➡️ Creating report {filename}.xlsx", fg="cyan")
            # Copy base template and select and rename deisred sheets
            report = Report(
                stakeholder=stakeholder,
                base_template_path=BASE_TEMPLATE,
                output_path=output_path / f"{filename}.xlsx"
            )        
        
        pass
    
# def process_stakeholder(stakeholder_type: str, output_dir: Path, ds: str, region: Optional[str] = None):
#     if stakeholder_type not in ["ministere", "drac"]:
#         typer.secho("❌ Invalid stakeholder type. Choose from [ministere, drac]", fg="red")
#         raise typer.Exit(code=1)
#     # Create Stakeholder instance
#     stakeholder = Stakeholder(
#         type=StakeholderType.MINISTERE if stakeholder_type == "ministere" else StakeholderType.DRAC,
#         name=region if region else "Ministère",
#     )
    
#     typer.secho(f"➡️ Processing reports for {stakeholder.type.value} - {stakeholder.name}", fg="cyan")
    
#     # Generate reports based on stakeholder's desired reports
#     for report_key in stakeholder.desired_reports:
#         if report_key not in REPORTS:
#             typer.secho(f"❌ Unknown report key: {report_key}", fg="red")
#             raise typer.Exit(code=1)
        
#         # Copy base template and select and rename deisred sheets
#         report = Report(
#             stakeholder=stakeholder,
#             base_template_path=BASE_TEMPLATE,
#             output_path=output_dir / f"{report_key}_{slugify(stakeholder.name)}.xlsx"
#         )
#         # Generate sheets as per the report configuration
#         for sheet_info in report_config["sheets"]:
#             sheet_type = SheetType.KPIS if "KPIs" in sheet_info["sheet"] else (SheetType.TOP if "Top" in sheet_info["sheet"] else SheetType.LEXIQUE)
#             sheet = report.add_sheet(sheet_type, sheet_info["name"], sheet_info["filter"], sheet_info["config"])
#             sheet.fill_with_data(conn=None, stakeholder=stakeholder, ds=ds)  # conn would be your DB connection
        
#         # save the report

#         report_name = f"{report_key}_{slugify(stakeholder.name)}.xlsx"
#         report_config = REPORTS[report_key]
#         report.save()