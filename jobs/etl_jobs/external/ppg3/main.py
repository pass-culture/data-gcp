from pathlib import Path
from typing import List, Optional
import typer
import logging
from datetime import date

from utils.data_utils import get_available_regions, ExportSession, Stakeholder, StakeholderType
from utils.file_utils import start_of_current_month, slugify, get_dated_base_dir

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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


REPORT_BASE_DIR_DEFAULT = Path("./reports")
report_base_dir = REPORT_BASE_DIR_DEFAULT





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
        typer.secho("❌ Invalid stakeholder. Choose from [all, ministere, drac]", fg="red")
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
        typer.secho(f"➡️ Generating reports for ministère and all regions", fg="cyan")
    elif stakeholder == "ministere":
        national = True
        selected_regions = None
        typer.secho(f"➡️ Generating reports for ministère", fg="cyan")
    # Special handling if stakeholder = drac
    elif stakeholder == "drac":
        national = False
        selected_regions = drac_selector(target)
        
    base_dir = get_dated_base_dir(report_base_dir, ds)
    # create_directory_structure(base_dir, selected_regions, national)
    
    #export session:
    try:
        with ExportSession(ds) as session:
            # load data
            session.load_data()
            # generate reports
            if national:
                # session.process_stakeholder_old("ministere", "ministere", base_dir / "NATIONAL", ds)
                stakeholder = Stakeholder(name="Ministère", type=StakeholderType.MINISTERE)
                session.process_stakeholder("ministere",stakeholder, base_dir / "NATIONAL", ds)
            if selected_regions:
                for region in selected_regions:
                    session.process_stakeholder("drac",region, base_dir / "REGIONAL" / region, ds)
    except Exception as e:
        logger.error(f"❌ Export session failed: {e}")
        raise
    
if __name__ == "__main__":
    app()
    
