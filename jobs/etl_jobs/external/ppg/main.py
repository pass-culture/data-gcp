import typer
from pathlib import Path
from configs import (
    TEMPLATE_DEFAULT,
    REPORT_BASE_DIR_DEFAULT,
)
from utils import (
    get_dated_base_dir,
    report_outdir,
)
from core import Report, ExportSession, SheetType
from datetime import date, datetime, timedelta
import logging



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def export_ppg(
    template_path: Path = typer.Option(TEMPLATE_DEFAULT, "--template", "-t", exists=True),
    report_base_dir: Path = typer.Option(REPORT_BASE_DIR_DEFAULT, "--report-dir"),
    target: str = typer.Option("all", "--target"),
    scope: str = typer.Option("all", "--scope", "-s"), 
    ds: str = typer.Option("2025-08-01", "--ds"),
):
    """Generate reports for different geographic targets and scopes."""
    
    print(f"🚀 Starting export: target={target}, scope={scope}")
    
    try:
        with ExportSession(ds, template_path, report_base_dir,scope,target) as session:
            # Step 1: Validate inputs
            session.validate_inputs(target, scope)
            
            # Step 2: Load data 
            print("📊 Loading data into DuckDB...")
            session.load_data()
            
            # Step 3: Create stakeholders (now includes multiple reports per DRAC)
            print("🗺️  Creating stakeholder list...")
            stakeholders = session.create_stakeholders(target)
            print(f"    Found {len(stakeholders)} stakeholder-report combinations")

            # Step 4: Generate reports for each stakeholder
            total_reports = len(stakeholders)
            successful_reports = 0
            failed_reports = 0
            
            for i, stakeholder in enumerate(stakeholders, 1):
                print(f"\n[{i}/{total_reports}] Processing: {stakeholder.name} ({stakeholder.type.value})")
                
                for report_type in stakeholder.desired_reports:
                    try:
                        print(f"    📋 Creating {report_type} report...")
                        
                        # Create report instance
                        report = Report(
                            target_name=stakeholder.name,
                            target_type=stakeholder.type,
                            ds=ds,
                        )
                        
                        # Add context for academy/department specific reports
                        if hasattr(stakeholder, 'academy_name'):
                            report.academy_filter = stakeholder.academy_name
                            print(f"       🏫 Academy: {stakeholder.academy_name}")
                        
                        if hasattr(stakeholder, 'department_name'):
                            report.department_filter = stakeholder.department_name
                            print(f"       🏛️  Department: {stakeholder.department_name}")

                        # Generate sheets configuration
                        report.generate_sheets_from_config(report_type)
                        report.generate_tab_names()
                        
                        # Set output path
                        dated_base = get_dated_base_dir(session.output_dir, ds)
                        outdir = report_outdir(dated_base, stakeholder)
                        outdir.mkdir(parents=True, exist_ok=True)
                        report.output_path = outdir / report.get_output_filename()
                        
                        print(f"       📄 Output: {report.output_path}")
                        print(f"       📊 Sheets: {len(report.sheets)}")
                        
                        # Generate the actual Excel file
                        print(f"       🏗️  Generating Excel file...")
                        report.generate_excel_report(session.conn, template_path)
                        
                        print(f"       ✅ Completed: {report.output_path}")
                        successful_reports += 1
                        
                    except Exception as e:
                        print(f"       ❌ Failed: {str(e)}")
                        logger.error(f"Report generation failed for {stakeholder.name} - {report_type}: {e}")
                        failed_reports += 1
                        continue
            
            # Final summary
            print(f"\n{'='*60}")
            print(f"🎯 Export Summary:")
            print(f"   ✅ Successful reports: {successful_reports}")
            print(f"   ❌ Failed reports: {failed_reports}")
            print(f"   📁 Output directory: {get_dated_base_dir(session.output_dir, ds)}")
            
            if failed_reports == 0:
                print("🎉 All reports generated successfully!")
            else:
                print(f"⚠️  {failed_reports} reports failed - check logs for details")
            
            print(f"{'='*60}")
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        logger.error(f"Export session failed: {e}")
        raise
if __name__ == "__main__":
    app()