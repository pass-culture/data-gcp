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

# BASE_TEMPLATE = Path("./jobs/etl_jobs/external/ppg/templates/PPG_Template_v4.xlsx")
BASE_TEMPLATE = Path("./templates/export_template_v4.xlsx")

TEMPLATE_DEFAULT = Path("./templates/export_template_v2.xlsx")



# data sources
table_prefix = "external_reporting"
# One Big Table (kpi per scope)
KPI_INDIV = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}individual"}
KPI_COLL = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}eac"}

TOP_INDIV_OFFER = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_offer"}
TOP_INDIV_OFFER_CAT = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_offer_category"}
TOP_INDIV_VENUE = {"dataset": BIGQUERY_ANALYTICS_DATASET, "table": f"{table_prefix}top_venue"}


# SHEETS = {
#     "Lexique": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "Lexique",
#         "data": None,
#         },
#     "Individual_KPIs": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "template_individuel",
#         "data": KPI_INDIV,
#         },
#     "Collective_KPIs": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "template_collective",
#         "data": KPI_COLL,
#         },
#     "Top_individual_Offer": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "template_top",
#         "data": TOP_INDIV_OFFER,
#         "headers": None
#         },
#     "Top_individual_Offer_Category": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "template_top",
#         "data": TOP_INDIV_OFFER_CAT,
#         },
#     "Top_individual_Venue": {
#         "template": TEMPLATE_DEFAULT,
#         "tab": "template_top",
#         "data": TOP_INDIV_VENUE,
#         },
# }

class SheetType(Enum):
    KPIS = "kpis"
    TOP = "top"
    LEXIQUE = "lexique"

SHEET_DEFINITIONS = {
    "individual_kpis": {
        "type": SheetType.KPIS,
        "template_tab": "template_individuel",
        "source_table": "individual",
    },
    "collective_kpis": {
        "type": SheetType.KPIS,
        "template_tab": "template_collectif",
        "source_table": "collective",
    },
    "lexique": {
        "type": SheetType.LEXIQUE,
        "template_tab": "Lexique",
        "source_table": None,
    },
    "top_offer": {
        "type": SheetType.TOP,
        "template_tab": "Top 50 offres",
        "source_table": "top_offer",
    },
    "top_offer_category": {
        "type": SheetType.TOP,
        "template_tab": "Top 50 par catégorie",
        "source_table": "top_offer_category",
    },
    "top_venue": {
        "type": SheetType.TOP,
        "template_tab": "Top 50 lieux",
        "source_table": "top_venue",
    },
}

REPORTS = {
    "national_summary": {
        "sheets": [
            {"definition": "lexique"},
            {"definition": "individual_kpis", "filters": {"scope": "individual", "scale": "national"}},
            {"definition": "top_offer", "filters": {"scope": "individual", "scale": "national"}},
            {"definition": "top_offer_category", "filters": {"scope": "individual", "scale": "national"}},
            {"definition": "top_venue", "filters": {"scope": "individual", "scale": "national"}},
            {"definition": "collective_kpis", "filters": {"scope": "collective", "scale": "national"}},
        ]
    },
    "region_summary": {
        "sheets": [
            {"definition": "lexique"},
            {"definition": "individual_kpis", "filters": {"scope": "individual", "scale": "national"}},
            {"definition": "individual_kpis", "filters": {"scope": "individual", "scale": "region"}},
            {"definition": "top_offer", "filters": {"scope": "individual", "scale": "region"}},
            {"definition": "top_offer_category", "filters": {"scope": "individual", "scale": "region"}},
            {"definition": "top_venue", "filters": {"scope": "individual", "scale": "region"}},
            {"definition": "collective_kpis", "filters": {"scope": "collective", "scale": "national"}},
            {"definition": "collective_kpis", "filters": {"scope": "collective", "scale": "region"}},

        ]
    },
    "academy_detail": {
        "sheets": [
            {"definition": "lexique"},
            {"definition": "collective_kpis", "filters": {"scope": "collective", "scale": "region"}},
            {"definition": "collective_kpis", "filters": {"scope": "collective", "scale": "academy"}},
        ]
    },
    "department_detail": {
        "sheets": [
            {"definition": "lexique"},
            {"definition": "individual_kpis", "filters": {"scope": "individual", "scale": "region"}},
            {"definition": "individual_kpis", "filters": {"scope": "individual", "scale": "department"}},
            {"definition": "top_offer", "filters": {"scope": "individual", "scale": "department"}},
            {"definition": "top_offer_category", "filters": {"scope": "individual", "scale": "department"}},
            {"definition": "top_venue", "filters": {"scope": "individual", "scale": "department"}},
        ]
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
    """Represents a sheet instance within a report."""
    definition: str                    # key from SHEET_DEFINITIONS
    template_tab: str                  # template tab name
    worksheet: Any                     # openpyxl worksheet instance
    filters: Dict[str, Any] = field(default_factory=dict)
    tab_name: Optional[str] = None
    stakeholder: Optional[Any] = None  # reference to Stakeholder
    context: Optional[Dict[str, Any]] = None  # region, academy, department, etc.

    def preprocess(self):
        """Placeholder for preprocessing formatting, column widths, titles."""
        pass

    def fill_data(self):
        """Placeholder for filling the sheet with data based on filters."""
        pass
    
    def postprocess(self):
        """Placeholder for postprocessing formatting."""
        pass


class Report:
    """Represents a single Excel report for a stakeholder."""
    
    def __init__(self, report_type: str, stakeholder, base_template_path: Path, output_path: Path, context: Dict[str, Any] = None):
        self.report_type = report_type
        self.stakeholder = stakeholder
        self.base_template_path = base_template_path
        self.output_path = output_path
        self.context = context or {}
        self.workbook: Any = None          # openpyxl.Workbook
        self.sheets: List[Sheet] = []      # list of Sheet instances

    def _load_template(self):
        """Load the template workbook directly as the report workbook."""
        self.workbook = openpyxl.load_workbook(self.base_template_path)
        
        # Remove default sheet if it exists and is empty
        if "Sheet" in self.workbook.sheetnames and len(self.workbook["Sheet"].rows) == 0:
            self.workbook.remove(self.workbook["Sheet"])

    def _copy_template_tab(self, template_tab: str, new_tab_name: str):
        template_ws = self.workbook_template[template_tab]
        new_ws = self.workbook.copy_worksheet(template_ws)
        new_ws.title = new_tab_name[:31]  # truncate if needed
        return new_ws
    
    def _build_tab_name(self, definition_key: str, filters: Dict[str, Any], context: Dict[str, Any]):
        """Return a user-friendly tab name based on sheet definition, filters, and context."""
        
        if definition_key == "individual_kpis":
            base = "Part Individuelle"
            scale = filters.get("scale", "").capitalize()
            return f"{base} ({scale})"
        
        elif definition_key == "collective_kpis":
            base = "Part Collective"
            scale = filters.get("scale", "").capitalize()
            return f"{base} ({scale})"
    
        else:
            tab_name = SHEET_DEFINITIONS.get(definition_key, {}).get("tab_name")
            if tab_name is None:
                tab_name = SHEET_DEFINITIONS.get(definition_key, {}).get("template_tab", definition_key)
            return tab_name.capitalize()
    
    def _resolve_sheets(self):
        """Rename and configure tabs from the template instead of copying across workbooks."""
        blueprint = REPORTS[self.report_type]
        for sheet_info in blueprint["sheets"]:
            definition_key = sheet_info["definition"]
            template_info = SHEET_DEFINITIONS[definition_key]
            
            # Decide tab name based on definition + filters + context
            tab_name = self._build_tab_name(
                definition_key, 
                {**sheet_info.get("filters", {}), **self.context},
                self.context
            )

            # Check if the tab already exists
            if tab_name in self.workbook.sheetnames:
                ws = self.workbook[tab_name]
            else:
                # Copy the template tab
                template_ws = self.workbook[template_info["template_tab"]]
                ws = self.workbook.copy_worksheet(template_ws)
       
            if len(tab_name) > 31:
                typer.secho(f"⚠️ Tab name '{tab_name}' exceeds 31 characters, truncating.", fg="yellow")
                tab_name = tab_name[:31] # truncate if too long
            ws.title = tab_name  
            
            # Create Sheet instance
            sheet = Sheet(
                definition=definition_key,
                template_tab=template_info["template_tab"],
                worksheet=ws,
                filters={**sheet_info.get("filters", {}), **self.context},
                tab_name=tab_name,
                stakeholder=self.stakeholder,
                context=self.context
            )
            self.sheets.append(sheet)

    def _cleanup_template_sheets(self, used_tabs: List[str]):
        """
        Remove all template/unused sheets and enforce blueprint order.
        """
        # Remove any sheet not in the used list
        for sheet_name in list(self.workbook.sheetnames):
            if sheet_name not in used_tabs:
                self.workbook.remove(self.workbook[sheet_name])

        # Enforce order from used_tabs
        for idx, tab_name in enumerate(used_tabs):
            ws = self.workbook[tab_name]
            self.workbook._sheets.remove(ws)
            self.workbook._sheets.insert(idx, ws)

                
    def build(self):
        """Build the report workbook with all sheets (copy templates, preprocess, fill data)."""
        self._load_template()
        self._resolve_sheets()
        self._cleanup_template_sheets([sheet.tab_name for sheet in self.sheets])

        # Preprocess and fill data for each sheet
        for sheet in self.sheets:
            sheet.preprocess()
            sheet.fill_data()

    def save(self):
        """Save the workbook to the output path."""
        if not self.output_path.suffix == ".xlsx":
            self.output_path = self.output_path.with_suffix(".xlsx")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.workbook.save(self.output_path)
        
class ReportPlanner:
    """Expands a stakeholder into the list of reports they should get."""

    def __init__(self, stakeholder):
        self.stakeholder = stakeholder

    def plan_reports(self) -> List[Dict[str, Any]]:
        """
        Returns a list of report jobs:
        {
            "report_type": str,
            "context": dict,  # region, academy, department, etc.
            "output_path": Path
        }
        """
        jobs = []

        if self.stakeholder.type.name == "MINISTERE":
            # Ministère gets only the national summary
            # base_path = Path("NATIONAL")
            jobs.append(
                {
                    "report_type": "national_summary",
                    "context": {},
                    "output_path": "rapport_national.xlsx"
                }
            )
            return jobs

        # DRAC gets:
        # 1) Regional summary (includes national for comparison)
        region_name = self.stakeholder.name
        # base_path = Path("REGIONAL") / f"{region_name}"
        jobs.append(
            {
                "report_type": "region_summary",
                "context": {"region": region_name},
                "output_path": "rapport_regional.xlsx"
            }
            )

        # 2) One detailed report per académie (collective scope)
        if self.stakeholder.academy_tree:
            for node in self.stakeholder.academy_tree.all_nodes():
                if node.tag not in ["root_acad", "Academies"]:
                    jobs.append(
                        {
                            "report_type": "academy_detail",
                            "context": {"region": region_name, "academy": node.tag},
                            "output_path": f"academie_{node.tag}.xlsx"
                        }
                    )

        # 3) One detailed report per département (individual scope)
        if self.stakeholder.department_tree:
            for node in self.stakeholder.department_tree.all_nodes():
                if node.tag not in ["root_dept", "Departements"]:
                    jobs.append(
                        {
                            "report_type": "department_detail",
                            "context": {"region": region_name, "department": node.tag},
                            "output_path": f"departement_{node.tag}.xlsx"
                        }
                    )

        return jobs