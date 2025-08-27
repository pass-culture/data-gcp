from typing import List, Optional, Iterable, Union
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime, date
import re
import string
import unicodedata

logger = logging.getLogger(__name__)


# files utils
def get_dated_base_dir(base_dir: Path) -> Path:
    """Create date-stamped base directory: reports_yyyymmdd"""
    today = date.today()
    dated_folder = f"reports_{today.strftime('%Y%m%d')}"
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

def build_outdir(base: Path, label: str | None = None) -> Path:
    """Build output directory path based on label"""
    if label:
        return base / slugify(label)
    return base


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
# xlsx utils
