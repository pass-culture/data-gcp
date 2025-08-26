from utils.models import Report, TargetStakeholder
from typing import List, Optional
from pathlib import Path
import duckdb

class ExportSession:
    """Manages the entire export session with proper resource cleanup."""
    
    def __init__(self, ds: str, template_path: Path, output_dir: Path):
        self.ds = ds
        self.template_path = template_path
        self.output_dir = output_dir
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.hierarchy_data = None
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def validate_inputs(self, target: str, scope: str):
        """Validate all inputs before starting."""
        if target not in ("national", "regional", "departemental", "all"):
            raise ValueError(f"Invalid target: {target}")
        if scope not in ("individual", "collective", "all"):
            raise ValueError(f"Invalid scope: {scope}")
        if not self.template_path.exists():
            raise FileNotFoundError(f"Template not found: {self.template_path}")
    
    def load_data(self, scopes: List[str]):
        """Load data into DuckDB."""
        pass
    
    def create_stakeholders(self, target: str) -> List[TargetStakeholder]:
        """Create stakeholder list based on target."""
        stakeholders = []
        
        if target in ("national", "all"):
            stakeholders.append(TargetStakeholder(
                name="Ministere",
                dimension_name="NAT", 
                dimension_value="NAT",
                level="national",
                scopes=["individual", "collective"]
            ))
        
        if target in ("regional", "departemental", "all"):
            # Load hierarchy if not already loaded
            if not self.hierarchy_data:
                from utils.hierarchy import build_region_hierarchy
                self.hierarchy_data = build_region_hierarchy()
            
            # Create regional stakeholders
            for region_name in self.hierarchy_data.keys():
                if target in ("regional", "all"):
                    stakeholders.append(TargetStakeholder(
                        name=f"DRAC {region_name}",
                        dimension_name="REG",
                        dimension_value=region_name,
                        level="regional",
                        scopes=["individual", "collective"],
                        region_name=region_name
                    ))
                
                # Create departmental stakeholders
                if target in ("departemental", "all"):
                    hierarchy = self.hierarchy_data[region_name]
                    for academy, departments in hierarchy.get("academy_departments", {}).items():
                        for department in departments:
                            stakeholders.append(TargetStakeholder(
                                name=f"Department {department}",
                                dimension_name="DEP",
                                dimension_value=department,
                                level="departemental",
                                scope="individual",
                                region_name=region_name,
                                academie_name=academy
                            ))
        
        return stakeholders
    
    def create_reports(self, stakeholder: List[TargetStakeholder], scope: str) -> List[Report]:
        """Create report objects for all stakeholders."""
        reports = []
        
        if scope != "all" and scope != stakeholder.scope:
            continue
        
        # Create output path
        output_path = self._get_output_path(stakeholder,scope)
        
        # Create report
        report = Report(
            stakeholder=stakeholder,
            template_path=self.template_path,
            output_path=output_path,
            scope = scope,
            sheets=[]  # Will be populated when workbook is loaded
        )
        
        reports.append(report)
        
        return reports
    
    def _get_output_path(self, stakeholder: TargetStakeholder) -> Path:
        """Generate output path for stakeholder."""
        dated_dir = get_dated_base_dir(self.output_dir, self.ds)
        
        if stakeholder.level == "national":
            return dated_dir / "NATIONAL" / stakeholder.get_output_filename()
        elif stakeholder.level == "regional":
            return dated_dir / "REGIONAL" / stakeholder.region_name / stakeholder.get_output_filename()
        elif stakeholder.level == "departemental":
            return dated_dir / "REGIONAL" / stakeholder.region_name / "ACADEMIES" / stakeholder.academie_name / stakeholder.get_output_filename()
        
        return dated_dir / stakeholder.get_output_filename()
    
    def generate_all_reports(self, reports: List[Report]):
        """Generate all reports with progress tracking."""
        total = len(reports)
        
        for i, report in enumerate(reports, 1):
            try:
                print(f"[{i}/{total}] Generating {report.stakeholder.name}...")
                report.generate(self.conn, self.ds)
                print(f"    ✅ Saved to {report.output_path}")
            except Exception as e:
                print(f"    ❌ Failed: {e}")
                continue