from typing import List, Optional, Iterable, Union, Dict
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, date
import re
import string
from enum import Enum
import unicodedata
from configs import REGION_HIERARCHY_TABLE, BIGQUERY_ANALYTICS_DATASET, GCP_PROJECT
from google.cloud import bigquery
import typer


logger = logging.getLogger(__name__)


# files utils
def get_dated_base_dir(base_dir: Path,ds:str) -> Path:
    """Create date-stamped base directory: reports_yyyymmdd"""
    dated_folder = f"reports_{ds.replace('-','')}"
    return base_dir / dated_folder

def safe_fs_name(name: str) -> str:
    """Make a safe folder name preserving French characters (max ~50 chars)."""
    # Only replace characters that are actually problematic for filesystems
    problematic_chars = r'[<>:"/\\|?*]'
    cleaned = re.sub(problematic_chars, "_", name).strip()
    return (cleaned or "default")[:50]

def slugify(value: str) -> str:
    """Filesystem-safe, ASCII-ish name preserving hyphens/underscores/dots."""
    nfkd = unicodedata.normalize("NFKD", value)
    ascii_str = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    safe = []
    for ch in ascii_str:
        if ch.isalnum() or ch in ("-", "_", "."):
            safe.append(ch)
        else:
            safe.append("_")
    s = "".join(safe).strip("._-")
    return s or "default"

def report_outdir(dated_base: Path, stakeholder) -> Path:
    """
    Return the folder for a given stakeholder under:
      reports_YYYYMMDD/NATIONAL
      or
      reports_YYYYMMDD/REGIONAL/<Region (accent-preserving)>
    Accepts Stakeholder or any object with .type and .name.
    """
    stype = str(getattr(stakeholder.type, "value", stakeholder.type)).lower()
    if stype == "ministere":
        return dated_base / "NATIONAL"
    # DRAC → REGIONAL/<Region> (folder keeps accents, just replaces unsafe FS chars)
    return dated_base / "REGIONAL" / safe_fs_name(stakeholder.name)

# # ---------- Scope-aware folder creation (national xor regional) ----------

# def reports_root(output_dir: Path, ds: str) -> Path:
#     """reports/reports_YYYYMMDD under output_dir."""
#     return get_dated_base_dir(output_dir, ds)

# def _ensure_dir(p: Path) -> Path:
#     p.mkdir(parents=True, exist_ok=True)
#     return p

# def national_folder(output_dir: Path, ds: str) -> Path:
#     return _ensure_dir(reports_root(output_dir, ds) / "NATIONAL")

# def regional_root(output_dir: Path, ds: str) -> Path:
#     return _ensure_dir(reports_root(output_dir, ds) / "REGIONAL")

# def region_folder(output_dir: Path, ds: str, region_name: str) -> Path:
#     """REGIONAL/<Region> — folder names keep accents via safe_fs_name()."""
#     return _ensure_dir(regional_root(output_dir, ds) / safe_fs_name(region_name))

# # ---------- File path builders (files use ASCII-ish names via slugify) ----------

# def national_report_path(output_dir: Path, ds: str) -> Path:
#     return national_folder(output_dir, ds) / "rapport_national.xlsx"

# def regional_global_path(output_dir: Path, ds: str, region_name: str) -> Path:
#     return region_folder(output_dir, ds, region_name) / f"global_{slugify(region_name)}.xlsx"

# def academy_report_path(output_dir: Path, ds: str, region_name: str, academy_name: str) -> Path:
#     return region_folder(output_dir, ds, region_name) / f"academie_{slugify(academy_name)}.xlsx"

# def department_report_path(output_dir: Path, ds: str, region_name: str, department_name: str) -> Path:
#     return region_folder(output_dir, ds, region_name) / f"{slugify(department_name)}.xlsx"

# def resolve_report_output_path(
#     output_dir: Path,
#     ds: str,
#     target_type: Union[str, Enum],
#     target_name: str,
#     filename: str,
# ) -> Path:
#     """
#     Put 'ministere' into NATIONAL; 'drac' into its region folder.
#     """
#     t = str(getattr(target_type, "value", target_type)).lower()
#     if t == "ministere":
#         return national_folder(output_dir, ds) / filename
#     # drac / regional
#     return region_folder(output_dir, ds, target_name) / filename

# def create_required_folders(
#     output_dir: Path,
#     ds: str,
#     target: str,                       # 'national' | 'drac' | 'all'
#     regions: Optional[Iterable[str]] = None,
# ) -> None:
#     t = (target or "").lower()
#     if t not in {"national", "drac", "all"}:
#         raise ValueError(f"Unknown target: {target}")

#     if t in {"national", "all"}:
#         national_folder(output_dir, ds)

#     if t in {"drac", "all"}:
#         if regions is None:
#             raise ValueError("regions must be provided when target is 'drac' or 'all'.")
#         for region in regions:
#             region_folder(output_dir, ds, region)

# def build_outdir(base: Path, label: str | None = None) -> Path:
#     """Build output directory path based on label"""
#     if label:
#         return base / slugify(label)
#     return base


# def build_filename(report_type: str, **kwargs) -> str:
#     """Build filename based on report type and parameters"""
#     config = REPORT_TYPES.get(report_type)
#     if not config:
#         raise ValueError(f"Unknown report type: {report_type}")

#     filename_template = config["filename"]

#     # Replace placeholders in filename
#     replacements = {
#         "region": kwargs.get("label", ""),
#         "academy": kwargs.get("label", ""),
#         "departement": kwargs.get("label", ""),
#     }

#     filename = filename_template
#     for key, value in replacements.items():
#         if f"{{{key}}}" in filename:
#             filename = filename.replace(f"{{{key}}}", slugify(value) if value else key)

#     return filename

# DuckDB & SQL utils

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



class HierarchyError(Exception):
    """Exception raised when hierarchy operations fail."""

    pass


def build_region_hierarchy(
    project_id: str = "passculture-data-prod",#GCP_PROJECT,
    dataset: str = "analytics_prod",#BIGQUERY_ANALYTICS_DATASET,
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
        raise HierarchyError(f"Failed to build region hierarchy: {e}")