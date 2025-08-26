import typer
from pathlib import Path
from utils.config import (
    TEMPLATE_DEFAULT,
    REPORT_BASE_DIR_DEFAULT,
)

from utils.models import TargetStakeholder, Report, Sheet
from datetime import date, datetime, timedelta

from utils.services import ExportSession

app = typer.Typer()


@app.command()
def export_ppg(
    template_path: Path = typer.Option(..., "--template", "-t", exists=True),
    report_base_dir: Path = typer.Option(..., "--report-dir"),
    target: str = typer.Option("all", "--target"),
    scope: str = typer.Option("all", "--scope", "-s"), 
    ds: str = typer.Option("2025-08-01", "--ds"),
):
    """Generate reports for different geographic targets and scopes."""
    
    print(f"🚀 Starting export: target={target}, scope={scope}")
    
    try:
        with ExportSession(ds, template_path, report_base_dir) as session:
            # Step 1: Validate inputs
            session.validate_inputs(target, scope)
            
            # Step 2: Load data 
            print("📊 Loading data into DuckDB...")
            scopes = ["individual", "collective"] if scope == "all" else [scope]
            session.load_data(scopes)
            
            # Step 3: Create stakeholders
            print("🗺️  Creating stakeholder list...")
            stakeholders = session.create_stakeholders(target)
            print(f"    Found {len(stakeholders)} stakeholders")
            
            # Step 4: Create reports
            for stakeholder in stakeholders:
                print(f"    - {stakeholder.name} ({stakeholder.level})")
                reports = session.create_reports(stakeholder, scope)
                print(f"    Created {len(reports)} reports")
            
            # Step 5: Generate reports
            print("📊 Generating reports...")
            session.generate_all_reports(reports)
            
            print("✅ Export completed successfully!")
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        raise

if __name__ == "__main__":
    app()