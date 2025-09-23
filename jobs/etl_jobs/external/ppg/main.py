import logging
from pathlib import Path
from typing import Optional

import typer

from config import REPORT_BASE_DIR_DEFAULT
from core import ExportSession, Stakeholder, StakeholderType
from utils.data_utils import drac_selector, get_available_regions, upload_zip_to_gcs
from utils.file_utils import (
    compress_directory,
    get_dated_base_dir,
    start_of_current_month,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


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
):
    """Generate reports for specified stakeholder."""

    # normalize case
    stakeholder = stakeholder.lower()

    if stakeholder not in ["all", "ministere", "drac"]:
        typer.secho(
            "❌ Invalid stakeholder. Choose from [all, ministere, drac]", fg="red"
        )
        raise typer.Exit(code=1)

    typer.secho(f"➡️ Stakeholder: {stakeholder}", fg="cyan")
    if target:
        typer.secho(f"➡️ Target: {target}", fg="cyan")
    if ds:
        typer.secho(f"➡️ DS: {ds}", fg="cyan")

    # Set up regions and national flags
    if stakeholder == "all":
        selected_regions = get_available_regions()
        national = True
        typer.secho("➡️ Generating reports for ministère and all regions", fg="cyan")
    elif stakeholder == "ministere":
        national = True
        selected_regions = None
        typer.secho("➡️ Generating reports for ministère", fg="cyan")
    elif stakeholder == "drac":
        national = False
        selected_regions = drac_selector(target)

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)

    # Generate reports
    try:
        with ExportSession(ds) as session:
            session.load_data()

            if national:
                stakeholder = Stakeholder(
                    name="Ministère", type=StakeholderType.MINISTERE
                )
                session.process_stakeholder(
                    "ministere", stakeholder, base_dir / "NATIONAL", ds
                )

            if selected_regions:
                for region in selected_regions:
                    session.process_stakeholder(
                        "drac", region, base_dir / "REGIONAL" / region, ds
                    )

        typer.secho(f"✅ Reports generated successfully in {base_dir}", fg="green")

    except Exception as e:
        logger.error(f"❌ Export session failed: {e}")
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

    if base_dir:
        source_dir = Path(base_dir)
    else:
        source_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)

    if not source_dir.exists():
        typer.secho(f"❌ Directory not found: {source_dir}", fg="red")
        raise typer.Exit(code=1)

    typer.secho(f"➡️ Compressing: {source_dir}", fg="cyan")

    try:
        zip_path = compress_directory(
            source_dir, REPORT_BASE_DIR_DEFAULT, clean_after_compression=clean
        )
        typer.secho(f"✅ Compressed to: {zip_path}", fg="green")

        if clean:
            typer.secho(f"🗑️ Cleaned source directory: {source_dir}", fg="yellow")

    except Exception as e:
        logger.error(f"❌ Compression failed: {e}")
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

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)
    zip_file = base_dir.with_suffix(".zip")
    typer.secho(f"➡️ Auto-detected zip file: {zip_file}", fg="cyan")

    if not zip_file.exists():
        typer.secho(f"❌ ZIP file not found: {zip_file}", fg="red")
        raise typer.Exit(code=1)

    if not zip_file.suffix.lower() == ".zip":
        typer.secho("⚠️ Warning: File doesn't have .zip extension", fg="yellow")

    typer.secho(f"➡️ Uploading: {zip_file}", fg="cyan")
    typer.secho(f"➡️ To bucket: {bucket}", fg="cyan")

    if destination:
        typer.secho(f"➡️ Destination: {destination}", fg="cyan")

    try:
        success = upload_zip_to_gcs(
            local_zip_path=zip_file, bucket_name=bucket, destination_name=destination
        )

        if success:
            typer.secho("✅ Upload completed successfully!", fg="green")
        else:
            typer.secho("❌ Upload failed", fg="red")
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
