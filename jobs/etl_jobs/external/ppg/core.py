from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Any
from enum import Enum
import duckdb
import openpyxl
import typer
from google.cloud import bigquery
import warnings

from configs import GCP_PROJECT, BIGQUERY_PPG_DATASET, SCOPE_TABLES, QUERIES, REPORTS, TOP_TABLES_BY_SCOPE, STAKEHOLDER_REPORTS, ALL_TABLES,TAB_NAME_MAX_LENGTH
from excel_handler import ExcelTemplateHandler
        

from utils import sanitize_date_fields, sanitize_numeric_types#, create_required_folders, resolve_report_output_path

class StakeholderType(Enum):
    MINISTERE = "ministere"
    DRAC = "drac"

class SheetType(Enum):
    KPIS = "kpis"
    TOP = "top"
    LEXIQUE = "lexique"

@dataclass
class Stakeholder:
    """Stakeholder - either Ministère or DRAC."""
    type: StakeholderType
    name: str
    desired_reports: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.desired_reports:
            self.desired_reports = list(STAKEHOLDER_REPORTS.get(self.type.value, []))

@dataclass
class Sheet:
    """Represents a sheet within a report."""
    type: SheetType  # kpis or top
    name: Optional[str] = None
    scale: Optional[str] = None  # national, regional, departmental, academie
    scope: Optional[str] = None  # individual, collective
    ds: Optional[str] = None  # consolidation date
    source_table: Optional[str] = None  # individual_data, collective_data, top_individual_offer_data
    format_config: Optional[Dict[str, Any]] = field(default_factory=dict)
    

    def generate_sheet_name(self) -> str:
        """Generate a descriptive sheet name."""
        if self.type == SheetType.LEXIQUE:
            self.name = "Lexique"
            return "Lexique"
        tab = ALL_TABLES.get(self.scope,{}).get("tab", "")
        scope = self.scope or ""
        scale = self.scale or ""
        assert (tab != ""), f"Tab name missing for source_table: {self.source_table}"
        assert (scope != ""), f"Scope missing for sheet with source_table: {self.source_table}"
        assert (scale != ""), f"Scale missing for sheet with source_table: {self.source_table}"
        if self.format_config.get("is_reference"):
            mascu_scope = "Individuel" if self.scope == "individual" else ("Collectif" if self.scope == "collective" else "")
            tab_name = f"Réf. {self.scale.capitalize()} - {mascu_scope}"

        else:
            tab_name = f"{tab} - {scale.capitalize()}"
        self.name = tab_name[:TAB_NAME_MAX_LENGTH]  # Excel sheet name limit
        print(f"Generated sheet name: {self.name}")
        return tab_name
        
    def get_table_name(self) -> str:
        """Get the DuckDB table name for this sheet."""
        return f"{self.source_table}"
    
    # def build_query(self, filters: Dict[str, str]) -> str:
    #     """Build SQL query for this sheet's data."""
    #     where_clauses = []
    #     for key, value in filters.items():
    #         where_clauses.append(f"{key} = '{value}'")
        
    #     where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
    #     return f"""
    #     SELECT * FROM {self.get_table_name()}
    #     WHERE {where_clause}
    #     ORDER BY partition_month, kpi_name
    #     """
        
    def build_query(self, filters: Dict[str, str]) -> str:
        """Build SQL query for this sheet's data with proper filtering."""
        where_clauses = ["1=1"]
        
        # Add base dimension filters
        for key, value in filters.items():
            if key and value:
                where_clauses.append(f"{key} = '{value}'")
        
        # Add academy-specific filters if needed
        if hasattr(self, 'academy_filter') and self.academy_filter:
            where_clauses.append(f"academy_name = '{self.academy_filter}'")
        
        # Add department-specific filters if needed
        if hasattr(self, 'department_filter') and self.department_filter:
            where_clauses.append(f"department_name = '{self.department_filter}'")
        
        where_clause = " AND ".join(where_clauses)
        
        # Choose query template based on source table
        if self.source_table and ("top_" in self.source_table or self.type == SheetType.TOP):
            query_template = """
            SELECT *
            FROM {table}
            WHERE {where_clause}
            ORDER BY partition_month, dimension_name, dimension_value
            """
        else:
            query_template = """
            SELECT partition_month, updated_at, dimension_name, dimension_value,
                kpi_name, numerator, denominator, kpi
            FROM {table}
            WHERE {where_clause}
            ORDER BY partition_month, dimension_name, dimension_value, kpi_name
            """
        
        return query_template.format(
            table=f"{self.get_table_name()}_data",
            where_clause=where_clause
        )
        
    def _fill_with_data(self, conn: duckdb.DuckDBPyConnection, stakeholder: Stakeholder, ds: str):
        """Fill this sheet with data for the stakeholder."""
        # Move your existing sheet filling logic here
        pass    
    
    def _apply_formatting(self):
        """Apply formatting to the worksheet."""
        pass
        
@dataclass
class Report:
    """Represents a complete report file."""
    target_name: str  # Name of target stakeholder
    target_type: StakeholderType
    ds: str  # consolidation date
    scopes: List[str] = field(default_factory=list)  # ["individual", "collective"]
    scales: List[str] = field(default_factory=list)  # ["national", "regional", "departmental","academie"]
    sheets: List[Sheet] = field(default_factory=list)
    output_path: Optional[Path] = None

    def __post_init__(self):
        if not self.sheets:
            self.sheets = []

    def generate_sheets_from_config(self, report_type: str):
        """Generate sheets based on REPORTS configuration."""
        if report_type not in REPORTS:
            raise ValueError(f"Unknown report type: {report_type}. Available: {list(REPORTS.keys())}")
        
        config = REPORTS[report_type]
        self.scopes = config["scopes"]
        self.scales = config["scales"]
        self.sheets = []

        if config.get("lexique", True):
            self.sheets.append(Sheet(type=SheetType.LEXIQUE))
            
        # Generate main sheets for each scale/scope combination
        for scope in self.scopes:
            # Add reference scale sheets if specified
            if "ref_scale" in config:
                self._add_sheets_for_scale_scope(config["ref_scale"], scope, is_reference=True)
            for scale in self.scales:
                self._add_sheets_for_scale_scope(scale, scope)
    
    
    def _add_sheets_for_scale_scope(self, scale: str, scope: str, is_reference: bool = False):
        """Add sheets for a specific scale/scope combination."""
        
        # Add KPI sheet
        source_table = scope #ALL_TABLES.get(scope,{}).get("table_name")
        
        self.sheets.append(Sheet(
            type=SheetType.KPIS,
            scale=scale,
            scope=scope,
            ds=self.ds,
            source_table=source_table,
            format_config={"is_reference": is_reference}
        ))
        
        # Add TOP sheets from config: Not applicable for reference scales
        if not is_reference and scope in TOP_TABLES_BY_SCOPE:
            for table in TOP_TABLES_BY_SCOPE[scope]:
                self.sheets.append(Sheet(
                    type=SheetType.TOP,
                    scale=scale,
                    scope=scope,
                    ds=self.ds,
                    source_table=table,
                    format_config={"is_reference": is_reference}
                ))
                
    def generate_tab_names(self):
        """Generate tab names for all sheets."""
        for sheet in self.sheets:
            sheet.generate_sheet_name()

    # def get_output_filename(self) -> str:
    #     """Generate output filename based on target and scales."""
    #     if self.target_type == StakeholderType.MINISTERE:
    #         return "rapport_national.xlsx"
    #     elif "regional" in self.scales:
    #         return f"résumé_{self.target_name}.xlsx"
    #     elif len(self.scales) == 1:
    #         return f"{self.scales[0]}_{self.target_name}.xlsx"
    #     else:
    #         return f"detail_{self.target_name}.xlsx"
    
    def get_data_filters(self) -> Dict[str, str]:
        """Get data filters based on target type and scales."""
        if self.target_type == StakeholderType.MINISTERE:
            return {"dimension_name": "NAT"}
        else:
            # For DRAC, filter by region
            return {"dimension_name": "REG", "dimension_value": self.target_name}
        

    def get_output_filename(self) -> str:
        """Generate output filename based on target, context, and scales."""
        if self.target_type == StakeholderType.MINISTERE:
            return "rapport_national.xlsx"
        
        # For DRAC stakeholders
        if hasattr(self, 'academy_filter') and self.academy_filter:
            # Academy-specific report
            from utils import slugify
            return f"académie_{slugify(self.academy_filter)}.xlsx"
        
        elif hasattr(self, 'department_filter') and self.department_filter:
            # Department-specific report
            from utils import slugify
            return f"départemental_{slugify(self.department_filter)}.xlsx"
        
        else:
            # Regional summary report
            return f"{self.target_name.replace(' ', '_')}.xlsx"

    def generate_excel_report(self, conn: duckdb.DuckDBPyConnection, template_path: Path):
        """Generate the actual Excel file using the template handler."""
        
        handler = ExcelTemplateHandler(template_path)
        handler.load_template()
        
        # Get data filters for this stakeholder/context
        filters = self.get_data_filters()
        
        # Apply context filters to sheets if needed
        for sheet in self.sheets:
            if hasattr(self, 'academy_filter'):
                sheet.academy_filter = self.academy_filter
            if hasattr(self, 'department_filter'):
                sheet.department_filter = self.department_filter
        
        try:
            # Fill each sheet with data
            for sheet in self.sheets:
                handler.fill_sheet_with_data(sheet, conn, filters)
            
            # Save the completed report
            handler.save_report(self.output_path)
            
        finally:
            handler.close()
        
# @dataclass
# class TargetStakeholder:
#     """Represents who gets a report and their filtering requirements."""
#     name: str   # Ministrere, DRAC
#     dimension_name: str  # NAT, REG
#     dimension_value: str
#     level: str  # national, regional, academie, departemental
#     scope: str  # individual, collective
#     region_name: Optional[str] = None
#     academie_name: Optional[str] = None
    
#     def get_data_filters(self) -> Dict[str, str]:
#         """Generate SQL WHERE clause filters."""
#         filters = {"dimension_name": self.dimension_name}
#         if self.dimension_value != self.dimension_name:  # Not national
#             filters["dimension_value"] = self.dimension_value
#         return filters
    
#     def get_output_filename(self) -> str:
#         """Generate appropriate filename."""
#         if self.level == "national":
#             return "rapport_national.xlsx"
#         elif self.level == "regional":
#             return f"rapport_{self.dimension_value}.xlsx"
#         elif self.level == "academie":
#             return f"collectif_academie_{self.dimension_value}.xlsx"
#         elif self.level == "departemental":
#             return f"individuel_{self.dimension_value}.xlsx"
#         return f"report_{self.name}.xlsx"

# @dataclass
# class Sheet:
#     """Represents a sheet within a report."""
#     type: str  # individuel, collectif, tops_individuel, tops_collectif
#     worksheet: openpyxl.worksheet.worksheet.Worksheet
#     kpi_filters: List[tuple]  # (row_idx, kpi_name, select_type, agg_type)
    
#     def fill_with_data(self, conn: duckdb.DuckDBPyConnection, stakeholder: TargetStakeholder, ds: str):
#         """Fill this sheet with data for the stakeholder."""
#         # Move your existing sheet filling logic here
#         pass
    
#     def _apply_formatting(self):
#         """Apply formatting to the worksheet."""
#         pass

# @dataclass
# class Report:
#     """Represents a complete report file."""
#     stakeholder: TargetStakeholder
#     base_template_path: Path
#     output_path: Path
#     scope: str = "all"   # individual, collective, all
#     sheets: List[Sheet] = None
#     workbook: Optional[openpyxl.Workbook] = None
    
#     def load_workbook_template(self):
#         """Load the Excel workbook."""
#         self.workbook = openpyxl.load_workbook(self.base_template_path)
    
#     def generate_sheets(self):
#         """Initialize sheets based on the template."""
#         if not self.workbook:
#             self.load_workbook()
        
        
#         self.sheets = []
        
#         for sheet_name in self.workbook.sheetnames:
#             worksheet = self.workbook[sheet_name]
#             # Determine type and kpi_filters based on sheet_name or content
#             type = "individual"
    
#     def generate(self, conn: duckdb.DuckDBPyConnection, ds: str):
#         """Generate the complete report."""
#         if not self.workbook:
#             self.load_workbook_template()
        
#         # Fill each sheet
#         for sheet in self.sheets:
#             sheet.fill_with_data(conn, self.stakeholder, ds)
        
#         # Apply final formatting and save
#         self._finalize_and_save()
    
#     def _finalize_and_save(self):
#         """Apply final formatting and save the report."""
#         # Move lexique sheet first, hide columns, etc.
#         self.output_path.parent.mkdir(parents=True, exist_ok=True)
#         self.workbook.save(self.output_path)




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
        self.output_dir.mkdir(parents=True, exist_ok=True)
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
    
    def create_stakeholders(self, target: str) -> List[Stakeholder]:
        """Create stakeholder list based on target - enhanced for multiple DRAC reports."""
        stakeholders = []
        
        if target in ("national", "all"):
            stakeholders.append(Stakeholder(
                type=StakeholderType.MINISTERE,
                name="Ministere"
            ))
        
        if target in ("drac", "all"):
            # Load hierarchy if not already loaded
            if not self.hierarchy_data:
                from utils import build_region_hierarchy
                self.hierarchy_data = build_region_hierarchy()
                
            for region_name, region_data in self.hierarchy_data.items():
                # 1. Create base DRAC stakeholder for regional summary
                regional_stakeholder = Stakeholder(
                    type=StakeholderType.DRAC,
                    name=region_name,
                    desired_reports=["regional_summary"]  # Global regional report
                )
                stakeholders.append(regional_stakeholder)
                
                # 2. Create academy-specific stakeholders for collective reports
                for academy in region_data["academies"]:
                    academy_stakeholder = Stakeholder(
                        type=StakeholderType.DRAC,
                        name=region_name,
                        desired_reports=["academie_detail"]
                    )
                    # Add academy context for filtering and naming
                    academy_stakeholder.academy_name = academy
                    academy_stakeholder.context_type = "academy"
                    stakeholders.append(academy_stakeholder)
                
                # 3. Create department-specific stakeholders for individual reports  
                for dept in region_data["departements"]:
                    dept_stakeholder = Stakeholder(
                        type=StakeholderType.DRAC, 
                        name=region_name,
                        desired_reports=["departemental_detail"]
                    )
                    # Add department context for filtering and naming
                    dept_stakeholder.department_name = dept
                    dept_stakeholder.context_type = "department"
                    stakeholders.append(dept_stakeholder)
        
        return stakeholders
    # def create_stakeholders(self, target: str) -> List[Stakeholder]:
    #     """Create stakeholder list based on target."""
    #     stakeholders = []
        
    #     if target in ("national", "all"):
    #         stakeholders.append(Stakeholder(
    #             type=StakeholderType.MINISTERE,
    #             name="Ministere"
    #         ))
        
    #     if target in ("drac", "all"):
    #         # Load hierarchy if not already loaded
    #         if not self.hierarchy_data:
    #             from utils import build_region_hierarchy
    #             self.hierarchy_data = build_region_hierarchy()
                
    #         for region_name in self.hierarchy_data.keys():
    #             stakeholders.append(Stakeholder(
    #                 type=StakeholderType.DRAC,
    #                 name=region_name
    #             ))
        
    #     return stakeholders



if __name__ == "__main__":
    report = Report(target_name="ile-de-france", target_type=StakeholderType.DRAC, ds="2025-08-01")
    report.generate_sheets_from_config("academie_detail")
    print(f"Generated {len(report.sheets)} sheets for report.")
    print(f"Generated {report.sheets} sheets for report.")

     
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