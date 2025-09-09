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
from utils.file_utils import slugify

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

def drac_selector(target: Optional[str]):
    """Special handling for stakeholder 'drac' with optional target regions."""
    regions = get_available_regions()
    slug_regions = {slugify(r): r for r in regions}
    selected_regions = []

    if target:
        input_slug_regions = target.split()
        typer.secho(f"➡️ Input regions: {', '.join(input_slug_regions)}", fg="cyan")
        invalid_regions = [r for r in input_slug_regions if r not in slug_regions.keys()]
        if invalid_regions:
            typer.secho(
                f"❌ Invalid region(s): {', '.join(invalid_regions)}. "
                f"Allowed regions: {', '.join(slug_regions.keys())}",
                fg="red",
            )
            raise typer.Exit(code=1)
        selected_regions = [slug_regions[r] for r in input_slug_regions]
        typer.secho(
            f"✅ selected region{'s' if len(selected_regions) > 1 else ''}: {', '.join(selected_regions)}",
            fg="green",
        )
    else:
        # Interactive selection
        typer.echo("Please choose one or more regions (space-separated numbers):")
        typer.echo("0. All regions")
        for i, r in enumerate(slug_regions.values(), start=1):
            typer.echo(f"{i}. {r}")

        choice = typer.prompt("Enter numbers")

        try:
            indices = [int(x) for x in choice.split()]
            if 0 in indices:
                selected_regions = regions[:]  # select all
            else:
                selected_regions = [regions[i - 1] for i in indices]
        except (ValueError, IndexError):
            typer.secho("❌ Invalid choice", fg="red")
            raise typer.Exit(code=1)

        typer.secho(
            f"✅ You selected regions: {', '.join(selected_regions)}", fg="green"
        )

    return selected_regions

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

