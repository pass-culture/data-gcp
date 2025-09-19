import logging

import typer

from config import REPORT_BASE_DIR_DEFAULT
from core import ExportSession, Stakeholder, StakeholderType
from utils.data_utils import drac_selector, get_available_regions
from utils.file_utils import get_dated_base_dir, start_of_current_month

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = typer.Typer()


@app.command()
def main(
    stakeholder: str = typer.Option(
        "all",
        "--stakeholder",
        "-s",
        help="Choose stakeholder: all, ministere, drac",
    ),
    ds: str = typer.Option(
        default_factory=start_of_current_month,  # dynamically set default
        help="Consolidation date in YYYY-MM-DD format",
    ),
    target: str = typer.Option(None, "--target", "-t", help="Target name"),
):
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

    # TO DO: hide later in
    if stakeholder == "all":
        selected_regions = get_available_regions()
        national = True
        typer.secho("➡️ Generating reports for ministère and all regions", fg="cyan")
    elif stakeholder == "ministere":
        national = True
        selected_regions = None
        typer.secho("➡️ Generating reports for ministère", fg="cyan")
    # Special handling if stakeholder = drac
    elif stakeholder == "drac":
        national = False
        selected_regions = drac_selector(target)

    base_dir = get_dated_base_dir(REPORT_BASE_DIR_DEFAULT, ds)
    # create_directory_structure(base_dir, selected_regions, national)

    # export session:
    try:
        with ExportSession(ds) as session:
            # load data
            session.load_data()
            # generate reports
            if national:
                # session.process_stakeholder_old("ministere", "ministere", base_dir / "NATIONAL", ds)
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
    except Exception as e:
        logger.error(f"❌ Export session failed: {e}")
        raise


if __name__ == "__main__":
    app()
