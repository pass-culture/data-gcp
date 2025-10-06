import re
import shutil
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List

from utils.verbose_logger import log_print


class FileUtilsError(Exception):
    """Exception raised when file operations fail."""

    pass


def start_of_current_month() -> str:
    today = date.today()
    return f"{today.year}-{today.month:02d}-01"


def to_first_of_month(ds: str) -> str:
    """Convert date string to first day of month."""
    if ds.endswith("01"):
        return ds

    first_of_month = ds[:8] + "01"  # YYYY-MM-DD -> YYYY-MM-01
    log_print.info(
        f"📅 Converting consolidation date {ds} -> {first_of_month} (first of month)"
    )
    return first_of_month


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


def get_dated_base_dir(base_dir: Path, ds: str = None) -> Path:
    """
    Create date-stamped base directory: reports_yyyymmdd

    Args:
        base_dir: Base reports directory
        ds: Export date in YYYY-MM-DD format (if None, uses today)

    Returns:
        Path to dated directory
    """
    if ds:
        date_obj = datetime.strptime(ds, "%Y-%m-%d").date()
    else:
        date_obj = date.today()

    dated_folder = f"reports_{date_obj.strftime('%Y%m%d')}"
    return base_dir / dated_folder


def _ensure_directory(path: Path) -> bool:
    """
    Ensure directory exists, create if it doesn't.

    Args:
        path: Directory path

    Returns:
        True if directory was created, False if it already existed
    """
    if path.exists():
        return False
    else:
        path.mkdir(parents=True, exist_ok=True)
        return True


def create_directory_structure(
    base_dir: Path, selected_regions: List = None, national: bool = True
) -> Dict[str, int]:
    """
    Create the complete directory structure for reports.

    Args:
        base_dir: Base directory for reports
        region_hierarchy: Region hierarchy data
        target_scope: Target scope (national, regional, departemental, all)

    Returns:
        Dictionary with creation statistics
    """
    stats = {"directories_created": 0, "directories_existing": 0}

    try:
        # National directory
        if national:
            national_dir = base_dir / "NATIONAL"
            if _ensure_directory(national_dir):
                stats["directories_created"] += 1
            else:
                stats["directories_existing"] += 1

        # Regional and academy directories
        if selected_regions:
            regional_base = base_dir / "REGIONAL"
            if _ensure_directory(regional_base):
                stats["directories_created"] += 1
            else:
                stats["directories_existing"] += 1

            for region in selected_regions:
                # Regional directory
                region_dir = regional_base / safe_fs_name(region)
                if _ensure_directory(region_dir):
                    stats["directories_created"] += 1
                else:
                    stats["directories_existing"] += 1

        log_print.info(
            f"📁 Directory structure created: {stats['directories_created']} new, {stats['directories_existing']} existing"
        )
        return stats

    except Exception as e:
        raise FileUtilsError(f"Failed to create directory structure: {e}")


def compress_directory(
    target_dir: Path, output_dir: Path, clean_after_compression: bool = False
):
    """
    Compress all files in the given directory using gzip.

    Args:
        directory: Directory to compress
        clean_after_compression: If True, delete original files after compression
    """
    if not target_dir.exists() or not target_dir.is_dir():
        raise FileUtilsError(
            f"Directory {target_dir} does not exist or is not a directory."
        )

    try:
        shutil.make_archive(f"{target_dir}", "zip", output_dir)
    except Exception as e:
        raise FileUtilsError(f"Failed to compress target_directory {target_dir}: {e}")
    log_print.info(f"🗜️ Compressed target_directory {target_dir} to {target_dir}.zip")
    if clean_after_compression:
        try:
            shutil.rmtree(target_dir)
            log_print.info(f"🧹 Cleaned up original directory {target_dir}")
        except Exception as e:
            raise FileUtilsError(
                f"Failed to clean original directory {target_dir}: {e}"
            )
    return f"{target_dir.parts[-1]}.zip"
