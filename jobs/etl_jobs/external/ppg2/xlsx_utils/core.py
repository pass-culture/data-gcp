from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Any
from enum import Enum

from treelib import Tree
import typer
import openpyxl
from config import (REGION_HIERARCHY_TABLE,GCP_PROJECT, BIGQUERY_ANALYTICS_DATASET, ENV_SHORT_NAME,)
from google.cloud import bigquery

from utils.file_utils import slugify

import os
from pathlib import Path


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

######## base configs
# GCP_PROJECT = os.environ.get("PROJECT_NAME", "passculture-data-prod")
# ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
# BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"


# REGION_HIERARCHY_TABLE = "region_department"

# TAB_NAME_MAX_LENGTH = 31

# ALL_TABLES = {
#     "individual": {"table_name": f"{table_prefix}individual", "tab": "Part individuelle", "scope": "individual"},
#     "collective": {"table_name": f"{table_prefix}collective", "tab": "Part collective", "scope": "collective"},
#     "top_individual_offer": {"table_name": f"{table_prefix}top_offer", "tab": "TOP 50 offres - individuel", "scope":"individual"},
#     "top_individual_offer_category": {"table_name": f"{table_prefix}top_offer_category", "tab": "TOP 50 offres par catégorie - individuel", "scope":"individual"},
#     "top_individual_venue": {"table_name": f"{table_prefix}top_venue", "tab": "TOP 50 lieux - individuel", "scope":"individual"},
# }




STAKEHOLDER_REPORTS = {
    "ministere": ["national_summary"],
    "drac": ["regional_summary", "departemental_detail", "academy_detail"]
}

BASE_TEMPLATE = Path("./jobs/etl_jobs/external/ppg/templates/PPG_Template_v4.xlsx")
TEMPLATE_DEFAULT = Path("./templates/export_template_v2.xlsx")



# data sources
table_prefix = "external_reporting"
# One Big Table (kpi per scope)
KPI_INDIV = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}individual"}
KPI_COLL = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}eac"}

TOP_INDIV_OFFER = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_offer"}
TOP_INDIV_OFFER_CAT = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_offer_category"}
TOP_INDIV_VENUE = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_venue"}


SHEETS = {
    "Lexique": {
        "template": TEMPLATE_DEFAULT,
        "tab": "Lexique",
        "data": None,
        },
    "Individual_KPIs": {
        "template": TEMPLATE_DEFAULT,
        "tab": "template_individuel",
        "data": KPI_INDIV,
        },
    "Collective_KPIs": {
        "template": TEMPLATE_DEFAULT,
        "tab": "template_collective",
        "data": KPI_COLL,
        },
    "Top_individual_Offer": {
        "template": TEMPLATE_DEFAULT,
        "tab": "template_top",
        "data": TOP_INDIV_OFFER,
        "headers": None
        },
    "Top_individual_Offer_Category": {
        "template": TEMPLATE_DEFAULT,
        "tab": "template_top",
        "data": TOP_INDIV_OFFER_CAT,
        },
    "Top_individual_Venue": {
        "template": TEMPLATE_DEFAULT,
        "tab": "template_top",
        "data": TOP_INDIV_VENUE,
        },
}


REPORTS = {
    "national_summary":{
        "sheets": [
            {"name": "Lexique", "sheet": "Lexique", "filter": {}, "config":{}},
            {"name": "Part Individuelle", "sheet":"Individual_KPIs","filter":{"scope":"individual","scale":"national"}, "config": {}},
            {"name": "Top 50 Offres", "sheet":"Top_individual_Offer","filter":{"scope":"individual","scale":"national"}, "config": {}},
            {"name": "Top 50 Offres par Catégorie", "sheet": "Top_individual_Offer_Category","filter": {"scope":"individual", "scale": "national"},"config":{}},
            {"name": "Top 50 Lieux", "sheet": "Top_individual_Venue", "filter": {"scope":"individual","scale":"national"}, "config":{}},
            {"name": "Part Collective", "sheet": "Collective_KPIs", "filter": {"scope": "collective", "scale": "national"}, "config":{}},
        ],
    },
    "regional_summary":{
        "sheets": [
            {"name": "Lexique", "sheet": "Lexique", "filter": {}, "config":{}},
            {"name": "Part Individuelle (national)", "sheet":"Individual_KPIs","filter":{"scope":"individual","scale":"national"}, "config": {}},
            {"name": "Part Individuelle", "sheet":"Individual_KPIs","filter":{"scope":"individual","scale":"regional"}, "config": {}},
            {"name": "Top 50 Offres", "sheet":"Top_individual_Offer","filter":{"scope":"individual","scale":"regional"}, "config": {}},
            {"name": "Top 50 Offres par Catégorie", "sheet": "Top_individual_Offer_Category","filter": {"scope":"individual", "scale": "regional"},"config":{}},
            {"name": "Top 50 Lieux", "sheet": "Top_individual_Venue", "filter": {"scope":"individual","scale":"regional"}, "config":{}},
            {"name": "Part Collective (national)", "sheet": "Collective_KPIs", "filter": {"scope": "collective", "scale": "national"}, "config":{}},
            {"name": "Part Collective", "sheet": "Collective_KPIs", "filter": {"scope": "collective", "scale": "regional"}, "config":{}},
        ],
    },
    "departemental_detail":{
        "sheets": [
            {"name": "Lexique", "sheet": "Lexique", "filter": {}, "config":{}},
            {"name": "Part Individuelle (regional)", "sheet":"Individual_KPIs","filter":{"scope":"individual","scale":"regional"}, "config": {}},
            {"name": "Part Individuelle", "sheet":"Individual_KPIs","filter":{"scope":"individual","scale":"departemental"}, "config": {}},
            {"name": "Top 50 Offres", "sheet":"Top_individual_Offer","filter":{"scope":"individual","scale":"departemental"}, "config": {}},
            {"name": "Top 50 Offres par Catégorie", "sheet": "Top_individual_Offer_Category","filter": {"scope":"individual", "scale": "departemental"},"config":{}},
            {"name": "Top 50 Lieux", "sheet": "Top_individual_Venue", "filter": {"scope":"individual","scale":"departemental"}, "config":{}},
        ],
    },
    "academy_detail":{
        "sheets": [
            {"name": "Lexique", "sheet": "Lexique", "filter": {}, "config":{}},
            {"name": "Part Collective (regional)", "sheet": "Collective_KPIs", "filter": {"scope": "collective", "scale": "regional"}, "config":{}},
            {"name": "Part Collective", "sheet": "Collective_KPIs", "filter": {"scope": "collective", "scale": "academie"}, "config":{}},
        ],
    },
}
# BASIC_QUERY_TEMPLATE = """
# SELECT partition_month, updated_at, dimension_name, dimension_value,
#        kpi_name, numerator, denominator, kpi
# FROM `{project}.{dataset}.{table}`
# WHERE 1=1
# ORDER BY partition_month, dimension_name, dimension_value, kpi_name
# """

# TOP_QUERY_TEMPLATE = """
# SELECT *
# FROM `{project}.{dataset}.{table}` 
# WHERE 1=1
# ORDER BY partition_month, dimension_name, dimension_value
# """
SOURCE_TABLES = {
    "individual": {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}_individual"},
    "collective": {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}_eac"},
    "top_offer": {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}_top_offer"},
    "top_offer_category": {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}_top_offer_category"},
    "top_venue": {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}_top_venue"},
}

class StakeholderType(Enum):
    MINISTERE = "ministere"
    DRAC = "drac"

class SheetType(Enum):
    KPIS = "kpis"
    TOP = "top"
    LEXIQUE = "lexique"

@dataclass
class Stakeholder:
    """Stakeholder - either Ministère (no children) or DRAC (region → academies & departments)."""
    type: StakeholderType
    name: str
    desired_reports: List[str] = field(default_factory=list)
    academy_tree: Optional[Tree] = None
    department_tree: Optional[Tree] = None

    def __post_init__(self):
        # Assign default reports
        if not self.desired_reports:
            self.desired_reports = list(STAKEHOLDER_REPORTS.get(self.type.value, []))

        # Only DRAC stakeholders get trees
        if self.type == StakeholderType.DRAC:
            # Fetch hierarchy from BigQuery
            hierarchy = build_region_hierarchy()

            # Ensure region exists
            if self.name not in hierarchy:
                raise ValueError(f"❌ Region '{self.name}' not found in hierarchy")

            scope = hierarchy[self.name]

            # Build departement tree
            self.department_tree = Tree()
            self.department_tree.create_node("Departements", "root_dept")
            for dep in scope["departements"]:
                self.department_tree.create_node(dep, f"{self.name}_dep_{dep}", parent="root_dept")

            # Build academy tree
            self.academy_tree = Tree()
            self.academy_tree.create_node("Academies", "root_acad")
            for acad in scope["academies"]:
                self.academy_tree.create_node(acad, f"{self.name}_acad_{acad}", parent="root_acad")

        else:
            self.academy_tree = None
            self.department_tree = None
        if self.type == StakeholderType.DRAC:
            typer.secho(f"➡️ DRAC '{self.name}' with adademy tree:\n{self.academy_tree.show(stdout=False)}", fg="cyan")
            typer.secho(f"➡️ DRAC '{self.name}' with department tree:\n{self.department_tree.show(stdout=False)}", fg="cyan")

@dataclass
class Sheet:
    """Represents a sheet within a report."""
    type: SheetType  # lexique, kpis or top
    name: Optional[str] = None
    scale: Optional[str] = None  # national, regional, departmental, academie
    scope: Optional[str] = None  # individual, collective
    ds: Optional[str] = None  # consolidation date
    source_table: Optional[str] = None  # individual_data, collective_data, top_individual_offer_data
    format_config: Optional[Dict[str, Any]] = field(default_factory=dict)


class Report:
    """Report - represents a complete report file."""
    def __init__(self,report_name: str, stakeholder: Stakeholder,base_template_path: Path,output_path: Path):
        self.report_name = report_name
        self.stakeholder = stakeholder
        self.base_template_path = base_template_path
        self.output_path = output_path
        self.workbook: Optional[Any] = None  # openpyxl.Workbook
        sheets: List[Any] = field(default_factory=list)
    # scope: str = "all"   # individual, collective, all
    # sheets: List[Any] = field(default_factory=list)
    # workbook: Optional[Any] = None  # openpyxl.Workbook
    
    def _load_workbook_template(self):
        """Load the workbook template."""
        self.workbook = openpyxl.load_workbook(self.base_template_path)
    
    def add_sheet(self, sheet_type: SheetType, name: str, data_filter: Dict[str, Any], config: Dict[str, Any]):
        """Add a sheet to the report."""
        if not self.workbook:
            self.load_workbook_template()
        
        sheet_info = SHEETS.get(f"{sheet_type.value.capitalize()}_{name.replace(' ', '_')}", None)
        if not sheet_info:
            raise ValueError(f"Sheet type {sheet_type} with name {name} not found in SHEETS configuration.")
        
        worksheet = self.workbook[sheet_info["tab"]]
        sheet = Sheet(sheet_type=sheet_type.value, worksheet=worksheet, kpi_filters=[])
        self.sheets.append(sheet)
        return sheet

        
    def _build_report(self):
        if not self.workbook:
            self._load_workbook_template()
        for sheet in self.sheets:
            ...
        
        pass
    # def save(self):
    #     """Save the report to the output path."""
    #     if not self.workbook:
    #         raise ValueError("Workbook is not loaded.")
    #     self.workbook.save(self.output_path)

