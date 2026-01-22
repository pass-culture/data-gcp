import concurrent.futures
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import typer

from config import (
    BASE_TEMPLATE,
    DATA_READER_PER_WRITER,
    GOOGLE_DRIVE_ROOT_FOLDER_ID,
    REPORT_BASE_DIR_DEFAULT,
    THREADS_SAFETY_MARGIN,
    WRITER_CONCURRENCY,
)
from core import (
    ExportSession,
    ReportPlanner,
    Stakeholder,
    StakeholderType,
    process_report_worker,
)
from services.google_drive import DriveUploadService
from services.tracking import GlobalStats
from utils.data_utils import (
    build_region_hierarchy,
    drac_selector,
    get_available_regions,
    upload_zip_to_gcs,
)
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
    concurrency: int = typer.Option(
        WRITER_CONCURRENCY,
        "--concurrency",
        "-c",
        help="Max simultaneous report generation workers",
    ),
    fetcher_concurrency: int = typer.Option(
        DATA_READER_PER_WRITER,
        "--fetcher-concurrency",
        help="Max threads per worker for DB fetching",
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

    # Calculate workers
    total_workers = int(min(concurrency, int(os.cpu_count() * THREADS_SAFETY_MARGIN)))
    if total_workers != concurrency:
        log_print.warning(
            f"⚠️  Adjusted concurrency to safety margin of {THREADS_SAFETY_MARGIN*100:.0f}% CPU cores:",
            fg="yellow",
        )
    log_print.info(
        f"🚀 Parallel Execution: {total_workers} workers",
        fg="cyan",
    )

    # Create global statistics tracker
    global_stats = GlobalStats()

    # Create temp directory for DB
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "reporting_data.duckdb")

        try:
            # Load Data
            log_print.info(f"💾 Loading data into temporary DB: {db_path}", fg="cyan")
            # Use ExportSession to load data
            # Note: we manually close session to release lock
            session = ExportSession(ds, db_path=db_path)
            session.load_data()
            if session.conn:
                session.conn.close()

            # Explicitly collect garbage after loading large datasets
            import gc

            gc.collect()

            # Build hierarchy once
            hierarchy = None
            if stakeholder != "ministere":  # drac or all
                hierarchy = build_region_hierarchy()

            # Determine stakeholders list
            stakeholders_to_process = []
            if national:
                stakeholders_to_process.append(("ministere", "Ministère"))
            if selected_regions:
                for r in selected_regions:
                    stakeholders_to_process.append(("drac", r))

            # Planning Phase
            tasks = []
            log_print.info("📋 Planning reports and copying templates...", fg="cyan")

            for s_type, s_name in stakeholders_to_process:
                # Create temporary stakeholder to plan
                # Pass hierarchy if drac
                s_hierarchy = hierarchy if s_type == "drac" else None

                try:
                    s_obj = Stakeholder(
                        type=StakeholderType(s_type),
                        name=s_name,
                        hierarchy_data=s_hierarchy,
                    )

                    planner = ReportPlanner(s_obj)
                    jobs = planner.plan_reports()

                    # Determine base output dir for this stakeholder
                    if s_type == "ministere":
                        s_base_dir = base_dir / "NATIONAL"
                    else:
                        s_base_dir = base_dir / "REGIONAL" / s_name

                    s_base_dir.mkdir(parents=True, exist_ok=True)

                    for job in jobs:
                        # Prepare destination
                        dest_path = s_base_dir / job["output_path"]
                        # Copy template
                        shutil.copy(BASE_TEMPLATE, dest_path)

                        # Create Task
                        task = {
                            "stakeholder_type": s_type,
                            "stakeholder_name": s_name,
                            "report_type": job["report_type"],
                            "context": job["context"],
                            "output_path": dest_path,
                            "ds": ds,
                            "db_path": db_path,
                            "fetcher_concurrency": fetcher_concurrency,
                            "verbose": state.verbose,
                        }
                        tasks.append(task)
                except Exception as e:
                    log_print.error(
                        f"❌ Failed to plan reports for {s_name}: {e}", fg="red"
                    )

            # Execution Phase
            log_print.info(f"⚡ Processing {len(tasks)} reports...", fg="cyan")

            with concurrent.futures.ProcessPoolExecutor(
                max_workers=total_workers,
            ) as executor:
                future_to_task = {
                    executor.submit(process_report_worker, task): task for task in tasks
                }

                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        stats = future.result(
                            timeout=300
                        )  # 5 minutes timeout per report result
                        # Add stats to global stats using our new helper method
                        global_stats.add_report_stats_to_stakeholder(
                            task["stakeholder_name"], task["stakeholder_type"], stats
                        )
                    except concurrent.futures.TimeoutError:
                        log_print.error(
                            f"❌ Task timed out for {task.get('output_path')}", fg="red"
                        )
                    except Exception as exc:
                        log_print.error(f"Task generated an exception: {exc}")

            log_print.info(
                f"✅ Reports generated successfully in {base_dir}", fg="green"
            )

        except Exception as e:
            log_print.error(f"❌ Export session failed: {e}")

        finally:
            # Always display and save statistics, even if execution failed partially
            if "global_stats" in locals():
                # Display comprehensive statistics
                global_stats.print_detailed_summary()

                # Optionally show detailed failure analysis
                if show_failures:
                    global_stats.print_failed_kpis_detail()

                if store_stats:
                    stats_file = (
                        REPORT_BASE_DIR_DEFAULT
                        / f"report_stats_{ds.replace('-', '')}.txt"
                    )
                    try:
                        global_stats.save_to_file(
                            stats_file, show_failures=show_failures
                        )
                        log_print.info(
                            f"📝 Statistics saved to {stats_file}", fg="cyan"
                        )
                    except Exception as save_err:
                        log_print.error(
                            f"❌ Failed to save statistics file: {save_err}", fg="red"
                        )


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
        "external_reporting",
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
):
    """Upload generated reports to Google Drive."""

    ds = to_first_of_month(ds)

    if not GOOGLE_DRIVE_ROOT_FOLDER_ID:
        log_print.error(
            "❌ Root folder ID required via GOOGLE_DRIVE_ROOT_FOLDER_ID conf var",
            fg="red",
        )
        raise typer.Exit(code=1)

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)
    if not base_dir.exists():
        log_print.error(f"❌ Reports directory not found: {base_dir}", fg="red")
        raise typer.Exit(code=1)

    log_print.info(f"➡️  Uploading from: {base_dir}", fg="cyan")

    try:
        drive_service = DriveUploadService()
        stats = drive_service.upload_reports_directory(
            base_dir, ds, GOOGLE_DRIVE_ROOT_FOLDER_ID
        )

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
