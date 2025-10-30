from pathlib import Path
from typing import Optional

import typer

from config import GOOGLE_DRIVE_ROOT_FOLDER_ID, REPORT_BASE_DIR_DEFAULT
from core import ExportSession, Stakeholder, StakeholderType
from services.tracking import GlobalStats
from utils.data_utils import drac_selector, get_available_regions, upload_zip_to_gcs
from utils.file_utils import (
    compress_directory,
    get_dated_base_dir,
    start_of_current_month,
    to_first_of_month,
)
from utils.verbose_logger import log_print, state

app = typer.Typer()


@app.callback()
def main(
    v: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
):
    """
    Global CLI callback. Sets verbose flag for all subcommands.
    """
    # Set global verbose state
    state.verbose = v
    log_print.set_verbose(v)

    if v:
        log_print.info("🔹 Verbose mode enabled")


@app.command()
def generate(
    stakeholder: str = typer.Option(
        "all",
        "--stakeholder",
        "-s",
        help="Choose stakeholder: all, ministere, drac",
    ),
    ds: str = typer.Option(
        default_factory=start_of_current_month,
        help="Consolidation date in YYYY-MM-DD format",
    ),
    target: str = typer.Option(None, "--target", "-t", help="Target name"),
    show_failures: bool = typer.Option(
        False, "--show-failures", "-f", help="Show detailed failure analysis"
    ),
    store_stats: bool = typer.Option(
        False, "--store-stats", help="Save statistics to report_stats.txt"
    ),
):
    """Generate reports for specified stakeholder."""

    ds = to_first_of_month(ds)
    # normalize case
    stakeholder = stakeholder.lower()

    if stakeholder not in ["all", "ministere", "drac"]:
        log_print.error(
            "❌ Invalid stakeholder. Choose from [all, ministere, drac]", fg="red"
        )
        raise typer.Exit(code=1)

    log_print.info(f"➡️  Stakeholder: {stakeholder}", fg="cyan")
    if target:
        log_print.info(f"➡️  Target: {target}", fg="cyan")
    if ds:
        log_print.info(f"➡️  DS: {ds}", fg="cyan")

    # Set up regions and national flags
    if stakeholder == "all":
        selected_regions = get_available_regions()
        national = True
        log_print.info("➡️  Generating reports for ministère and all regions", fg="cyan")
    elif stakeholder == "ministere":
        national = True
        selected_regions = None
        log_print.info("➡️  Generating reports for ministère", fg="cyan")
    elif stakeholder == "drac":
        national = False
        selected_regions = drac_selector(target)

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)

    # ===== NEW: Create global statistics tracker =====
    global_stats = GlobalStats()

    # Generate reports
    try:
        with ExportSession(ds) as session:
            session.load_data()

            if national:
                stakeholder_obj = Stakeholder(
                    name="Ministère", type=StakeholderType.MINISTERE
                )
                # ===== MODIFIED: Capture returned stats =====
                stakeholder_stats = session.process_stakeholder(
                    "ministere", stakeholder_obj, base_dir / "NATIONAL", ds
                )
                global_stats.add_stakeholder_stats(stakeholder_stats)

            if selected_regions:
                for region in selected_regions:
                    # ===== MODIFIED: Capture returned stats =====
                    stakeholder_stats = session.process_stakeholder(
                        "drac", region, base_dir / "REGIONAL" / region, ds
                    )
                    global_stats.add_stakeholder_stats(stakeholder_stats)

        log_print.info(f"✅ Reports generated successfully in {base_dir}", fg="green")

        # ===== NEW: Display comprehensive statistics =====
        global_stats.print_detailed_summary()

        # ===== NEW: Optionally show detailed failure analysis =====
        if show_failures:
            global_stats.print_failed_kpis_detail()

        if store_stats:
            stats_file = (
                REPORT_BASE_DIR_DEFAULT / f"report_stats_{ds.replace('-', '')}.txt"
            )
            global_stats.save_to_file(stats_file, show_failures=show_failures)
            log_print.info(f"📝 Statistics saved to {stats_file}", fg="cyan")

    except Exception as e:
        log_print.error(f"❌ Export session failed: {e}")
        raise typer.Exit(code=1)


@app.command()
def compress(
    ds: str = typer.Option(
        default_factory=start_of_current_month,
        help="Consolidation date in YYYY-MM-DD format (to locate directory)",
    ),
    base_dir: Optional[str] = typer.Option(
        None,
        "--base-dir",
        "-d",
        help="Base directory to compress (if different from default)",
    ),
    clean: bool = typer.Option(
        False, "--clean", "-C", help="Clean source directory after compression"
    ),
):
    """Compress generated reports directory."""

    ds = to_first_of_month(ds)
    if base_dir:
        source_dir = Path(base_dir)
    else:
        source_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)

    if not source_dir.exists():
        log_print.error(f"❌ Directory not found: {source_dir}", fg="red")
        raise typer.Exit(code=1)

    log_print.info(f"➡️  Compressing: {source_dir}", fg="cyan")

    try:
        zip_path = compress_directory(
            source_dir, REPORT_BASE_DIR_DEFAULT, clean_after_compression=clean
        )
        log_print.info(f"✅ Compressed to: {zip_path}", fg="green")

        if clean:
            log_print.info(f"🗑️ Cleaned source directory: {source_dir}", fg="yellow")

    except Exception as e:
        log_print.error(f"❌ Compression failed: {e}")
        raise typer.Exit(code=1)


@app.command()
def upload(
    ds: str = typer.Option(
        default_factory=start_of_current_month,
        help="Consolidation date in YYYY-MM-DD format (to find zip file)",
    ),
    bucket: Optional[str] = typer.Option(
        None, "--bucket", "-b", help="GCS bucket name"
    ),
    destination: Optional[str] = typer.Option(
        "ppg_reports",
        "--destination",
        "-d",
        help="Destination path in bucket (optional)",
    ),
):
    """Upload ZIP file to Google Cloud Storage."""

    ds = to_first_of_month(ds)
    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)
    zip_file = base_dir.with_suffix(".zip")
    log_print.info(f"➡️ Auto-detected zip file: {zip_file}", fg="cyan")

    if not zip_file.exists():
        log_print.error(f"❌ ZIP file not found: {zip_file}", fg="red")
        raise typer.Exit(code=1)

    if zip_file.suffix.lower() != ".zip":
        log_print.warning("⚠️ Warning: File doesn't have .zip extension", fg="yellow")

    log_print.info(f"➡️ Uploading: {zip_file}", fg="cyan")

    if destination:
        log_print.info(f"➡️ Destination: {destination}", fg="cyan")

    try:
        success = upload_zip_to_gcs(
            local_zip_path=zip_file, bucket_name=bucket, destination_name=destination
        )

        if success:
            log_print.info("✅ Upload completed successfully!", fg="green")
        else:
            log_print.error("❌ Upload failed", fg="red")
            raise typer.Exit(code=1)

    except Exception as e:
        log_print.error(f"❌ Upload failed: {e}")
        raise typer.Exit(code=1)


@app.command()
def upload_drive(
    ds: str = typer.Option(
        default_factory=start_of_current_month,
        help="Consolidation date YYYY-MM-DD",
    ),
    root_folder_id: Optional[str] = typer.Option(
        None,
        "--root-folder",
        "-r",
        help="Drive folder ID (uses env var if not set)",
    ),
):
    """Upload generated reports to Google Drive."""
    from services.google_drive import DriveUploadService

    ds = to_first_of_month(ds)
    folder_id = root_folder_id or GOOGLE_DRIVE_ROOT_FOLDER_ID

    if not folder_id:
        log_print.error(
            "❌ Root folder ID required via --root-folder or PPG_GOOGLE_DRIVE_FOLDER_ID env var",
            fg="red",
        )
        raise typer.Exit(code=1)

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)
    if not base_dir.exists():
        log_print.error(f"❌ Reports directory not found: {base_dir}", fg="red")
        raise typer.Exit(code=1)

    log_print.info(f"➡️  Uploading from: {base_dir}", fg="cyan")
    log_print.info(f"➡️  Drive folder: {folder_id}", fg="cyan")

    try:
        drive_service = DriveUploadService()
        stats = drive_service.upload_reports_directory(base_dir, ds, folder_id)

        log_print.info("✅ Upload complete!", fg="green")
        log_print.info(f"   Files uploaded: {stats['files_uploaded']}")
        log_print.info(f"   Folders created: {stats['folders_created']}")
        log_print.info(f"   Files skipped: {stats['files_skipped']}")

        if stats["errors"]:
            log_print.warning(f"⚠️  {len(stats['errors'])} errors occurred", fg="yellow")
            for error in stats["errors"]:
                log_print.error(f"   - {error}", fg="red")
            raise typer.Exit(code=1)

    except Exception as e:
        log_print.error(f"❌ Drive upload failed: {e}", fg="red")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
