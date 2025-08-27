# models.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Any
import duckdb
import openpyxl
import typer
from google.cloud import bigquery
import warnings

from configs import GCP_PROJECT, BIGQUERY_PPG_DATASET, SCOPE_TABLES, QUERIES
from utils import sanitize_date_fields, sanitize_numeric_types

@dataclass
class TargetStakeholder:
    """Represents who gets a report and their filtering requirements."""
    name: str   # Ministrere, DRAC
    dimension_name: str  # NAT, REG
    dimension_value: str
    level: str  # national, regional, academie, departemental
    scope: str  # individual, collective
    region_name: Optional[str] = None
    academie_name: Optional[str] = None
    
    def get_data_filters(self) -> Dict[str, str]:
        """Generate SQL WHERE clause filters."""
        filters = {"dimension_name": self.dimension_name}
        if self.dimension_value != self.dimension_name:  # Not national
            filters["dimension_value"] = self.dimension_value
        return filters
    
    def get_output_filename(self) -> str:
        """Generate appropriate filename."""
        if self.level == "national":
            return "rapport_national.xlsx"
        elif self.level == "regional":
            return f"rapport_{self.dimension_value}.xlsx"
        elif self.level == "academie":
            return f"collectif_academie_{self.dimension_value}.xlsx"
        elif self.level == "departemental":
            return f"individuel_{self.dimension_value}.xlsx"
        return f"report_{self.name}.xlsx"

@dataclass
class Sheet:
    """Represents a sheet within a report."""
    sheet_type: str  # individuel, collectif, tops_individuel, tops_collectif
    worksheet: openpyxl.worksheet.worksheet.Worksheet
    kpi_filters: List[tuple]  # (row_idx, kpi_name, select_type, agg_type)
    
    def fill_with_data(self, conn: duckdb.DuckDBPyConnection, stakeholder: TargetStakeholder, ds: str):
        """Fill this sheet with data for the stakeholder."""
        # Move your existing sheet filling logic here
        pass
    
    def _apply_formatting(self):
        """Apply formatting to the worksheet."""
        pass

@dataclass
class Report:
    """Represents a complete report file."""
    stakeholder: TargetStakeholder
    base_template_path: Path
    output_path: Path
    scope: str = "all"   # individual, collective, all
    sheets: List[Sheet] = None
    workbook: Optional[openpyxl.Workbook] = None
    
    def load_workbook_template(self):
        """Load the Excel workbook."""
        self.workbook = openpyxl.load_workbook(self.base_template_path)
    
    def generate_sheets(self):
        """Initialize sheets based on the template."""
        if not self.workbook:
            self.load_workbook()
        
        
        self.sheets = []
        
        for sheet_name in self.workbook.sheetnames:
            worksheet = self.workbook[sheet_name]
            # Determine sheet_type and kpi_filters based on sheet_name or content
            sheet_type = "individual"
    
    def generate(self, conn: duckdb.DuckDBPyConnection, ds: str):
        """Generate the complete report."""
        if not self.workbook:
            self.load_workbook_template()
        
        # Fill each sheet
        for sheet in self.sheets:
            sheet.fill_with_data(conn, self.stakeholder, ds)
        
        # Apply final formatting and save
        self._finalize_and_save()
    
    def _finalize_and_save(self):
        """Apply final formatting and save the report."""
        # Move lexique sheet first, hide columns, etc.
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.workbook.save(self.output_path)




class ExportSession:
    """Manages the entire export session with proper resource cleanup."""
    
    def __init__(self, ds: str, template_path: Path, output_dir: Path, scope: str, target: str):
        self.ds = ds
        self.template_path = template_path
        self.output_dir = output_dir
        self.scope: str = scope
        self.target = target
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.hierarchy_data = None
        
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def validate_inputs(self, target: str, scope: str):
        """Validate all inputs before starting."""
        if target.lower() not in ("national", "drac", "all"):
            raise ValueError(f"Invalid target: {target}")
        if scope not in ("individual", "collective", "all"):
            raise ValueError(f"Invalid scope: {scope}")
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found: {self.template_path}")
    
    def load_data(self):
        """Load tables based on self.scope."""
        if self.scope not in SCOPE_TABLES:
            raise ValueError(f"Unknown scope: {self.scope}. Available: {list(SCOPE_TABLES.keys())}")
        
        tables_to_load = SCOPE_TABLES[self.scope]
        
        client = bigquery.Client(project=GCP_PROJECT)
        self.conn = duckdb.connect(":memory:")
        
        for table_key in tables_to_load:
            if table_key not in QUERIES:
                typer.echo(f"Warning: No query defined for '{table_key}', skipping")
                continue
                
            self._load_table(client, table_key)
    
    def _load_table(self, client: bigquery.Client, table_key: str):
        """Load a single table into DuckDB."""
        query = QUERIES[table_key]
        table_name = f"{table_key}_data"
        
        typer.echo(f"Loading {table_key} from BigQuery...")
        
        
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*BigQuery Storage module not found.*")
            df = client.query(query).to_dataframe()
            typer.echo(f"{table_key} dtypes: {df.dtypes}")
        if df.empty:
            typer.echo(f"Warning: {table_key} returned no data")
            return
        
        df = sanitize_date_fields(df, "partition_month")
        df = sanitize_numeric_types(df)
            
        typer.echo(f"Loaded {len(df):,} records for {table_key}")
        
        # Register and create table
        self.conn.register(f"{table_key}_tmp", df)
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_key}_tmp")
        
        # Create indexes based on available columns
        self._create_indexes(table_name, df.columns.tolist())
        typer.echo(f"MOCK -> Created {table_name} table with indexes")

    
    def _create_indexes(self, table_name: str, columns: List[str]):
        """Create indexes on commonly queried columns."""
        pass

     
    # def create_stakeholders(self, target: str) -> List[TargetStakeholder]:
    #     """Create stakeholder list based on target."""
    #     stakeholders = []
        
    #     if target in ("national", "all"):
    #         stakeholders.append(TargetStakeholder(
    #             name="Ministere",
    #             dimension_name="NAT",
    #             dimension_value="NAT",
    #             level="national",
    #             scopes=["individual", "collective"]
    #         ))
        
    #     if target in ("regional", "departemental", "all"):
    #         # Load hierarchy if not already loaded
    #         if not self.hierarchy_data:
    #             from utils.hierarchy import build_region_hierarchy
    #             self.hierarchy_data = build_region_hierarchy()
            
    #         # Create regional stakeholders
    #         for region_name in self.hierarchy_data.keys():
    #             if target in ("regional", "all"):
    #                 stakeholders.append(TargetStakeholder(
    #                     name=f"DRAC {region_name}",
    #                     dimension_name="REG",
    #                     dimension_value=region_name,
    #                     level="regional",
    #                     scopes=["individual", "collective"],
    #                     region_name=region_name
    #                 ))
                
    #             # Create departmental stakeholders
    #             if target in ("departemental", "all"):
    #                 hierarchy = self.hierarchy_data[region_name]
    #                 for academy, departments in hierarchy.get("academy_departments", {}).items():
    #                     for department in departments:
    #                         stakeholders.append(TargetStakeholder(
    #                             name=f"Department {department}",
    #                             dimension_name="DEP",
    #                             dimension_value=department,
    #                             level="departemental",
    #                             scope="individual",
    #                             region_name=region_name,
    #                             academie_name=academy
    #                         ))
        
    #     return stakeholders
    
    # def create_reports(self, stakeholder: List[TargetStakeholder], scope: str) -> List[Report]:
    #     """Create report objects for all stakeholders."""
    #     reports = []
        
    #     if scope != "all" and scope != stakeholder.scope:
    #         continue
        
    #     # Create output path
    #     output_path = self._get_output_path(stakeholder,scope)
        
    #     # Create report
    #     report = Report(
    #         stakeholder=stakeholder,
    #         template_path=self.template_path,
    #         output_path=output_path,
    #         scope = scope,
    #         sheets=[]  # Will be populated when workbook is loaded
    #     )
        
    #     reports.append(report)
        
    #     return reports
    
    # def _get_output_path(self, stakeholder: TargetStakeholder) -> Path:
    #     """Generate output path for stakeholder."""
    #     dated_dir = get_dated_base_dir(self.output_dir, self.ds)
        
    #     if stakeholder.level == "national":
    #         return dated_dir / "NATIONAL" / stakeholder.get_output_filename()
    #     elif stakeholder.level == "regional":
    #         return dated_dir / "REGIONAL" / stakeholder.region_name / stakeholder.get_output_filename()
    #     elif stakeholder.level == "departemental":
    #         return dated_dir / "REGIONAL" / stakeholder.region_name / "ACADEMIES" / stakeholder.academie_name / stakeholder.get_output_filename()
        
    #     return dated_dir / stakeholder.get_output_filename()
    
    # def generate_all_reports(self, reports: List[Report]):
    #     """Generate all reports with progress tracking."""
    #     total = len(reports)
        
    #     for i, report in enumerate(reports, 1):
    #         try:
    #             print(f"[{i}/{total}] Generating {report.stakeholder.name}...")
    #             report.generate(self.conn, self.ds)
    #             print(f"    ✅ Saved to {report.output_path}")
    #         except Exception as e:
    #             print(f"    ❌ Failed: {e}")
    #             continue