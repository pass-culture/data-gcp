import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import typer

logger = logging.getLogger(__name__)


class KPIStatus(Enum):
    """Status of a KPI processing attempt."""

    SUCCESS = "success"
    FAILED = "failed"
    NO_DATA = "no_data"
    WRITE_FAILED = "write_failed"


class TopStatus(Enum):
    """Status of a top ranking processing attempt."""

    SUCCESS = "success"
    FAILED = "failed"
    NO_DATA = "no_data"


@dataclass
class KPIResult:
    """Detailed result for a single KPI."""

    kpi_name: str
    status: KPIStatus
    values_written: int = 0
    total_cells: int = 0
    error_message: Optional[str] = None
    sheet_name: Optional[str] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate for this KPI."""
        if self.total_cells == 0:
            return 0.0
        return (self.values_written / self.total_cells) * 100


@dataclass
class TopResult:
    """Detailed result for a top ranking."""

    top_name: str
    status: TopStatus
    rows_written: int = 0
    error_message: Optional[str] = None
    sheet_name: Optional[str] = None


@dataclass
class SheetStats:
    """Statistics for a single sheet."""

    sheet_name: str
    sheet_type: str  # 'kpis', 'top', 'lexique'
    kpi_results: List[KPIResult] = field(default_factory=list)
    top_results: List[TopResult] = field(default_factory=list)

    def add_kpi_result(self, result: KPIResult):
        """Add a KPI result to this sheet."""
        result.sheet_name = self.sheet_name
        self.kpi_results.append(result)

    def add_top_result(self, result: TopResult):
        """Add a top result to this sheet."""
        result.sheet_name = self.sheet_name
        self.top_results.append(result)

    @property
    def kpis_successful(self) -> int:
        return sum(1 for r in self.kpi_results if r.status == KPIStatus.SUCCESS)

    @property
    def kpis_failed(self) -> int:
        return sum(
            1
            for r in self.kpi_results
            if r.status in [KPIStatus.FAILED, KPIStatus.WRITE_FAILED]
        )

    @property
    def kpis_no_data(self) -> int:
        return sum(1 for r in self.kpi_results if r.status == KPIStatus.NO_DATA)

    @property
    def tops_successful(self) -> int:
        return sum(1 for r in self.top_results if r.status == TopStatus.SUCCESS)

    @property
    def tops_failed(self) -> int:
        return sum(1 for r in self.top_results if r.status == TopStatus.FAILED)

    @property
    def tops_no_data(self) -> int:
        return sum(1 for r in self.top_results if r.status == TopStatus.NO_DATA)


@dataclass
class ReportStats:
    """Statistics for a single report."""

    report_name: str
    report_type: str
    sheet_stats: List[SheetStats] = field(default_factory=list)

    def add_sheet_stats(self, stats: SheetStats):
        """Add sheet statistics to this report."""
        self.sheet_stats.append(stats)

    @property
    def total_kpis(self) -> int:
        return sum(len(s.kpi_results) for s in self.sheet_stats)

    @property
    def kpis_successful(self) -> int:
        return sum(s.kpis_successful for s in self.sheet_stats)

    @property
    def kpis_failed(self) -> int:
        return sum(s.kpis_failed for s in self.sheet_stats)

    @property
    def kpis_no_data(self) -> int:
        return sum(s.kpis_no_data for s in self.sheet_stats)

    @property
    def total_tops(self) -> int:
        return sum(len(s.top_results) for s in self.sheet_stats)

    @property
    def tops_successful(self) -> int:
        return sum(s.tops_successful for s in self.sheet_stats)

    @property
    def tops_failed(self) -> int:
        return sum(s.tops_failed for s in self.sheet_stats)

    @property
    def tops_no_data(self) -> int:
        return sum(s.tops_no_data for s in self.sheet_stats)


@dataclass
class StakeholderStats:
    """Statistics for a single stakeholder."""

    stakeholder_name: str
    stakeholder_type: str
    report_stats: List[ReportStats] = field(default_factory=list)

    def add_report_stats(self, stats: ReportStats):
        """Add report statistics to this stakeholder."""
        self.report_stats.append(stats)

    @property
    def total_reports(self) -> int:
        return len(self.report_stats)

    @property
    def total_kpis(self) -> int:
        return sum(r.total_kpis for r in self.report_stats)

    @property
    def kpis_successful(self) -> int:
        return sum(r.kpis_successful for r in self.report_stats)

    @property
    def kpis_failed(self) -> int:
        return sum(r.kpis_failed for r in self.report_stats)

    @property
    def kpis_no_data(self) -> int:
        return sum(r.kpis_no_data for r in self.report_stats)

    @property
    def total_tops(self) -> int:
        return sum(r.total_tops for r in self.report_stats)

    @property
    def tops_successful(self) -> int:
        return sum(r.tops_successful for r in self.report_stats)

    @property
    def tops_failed(self) -> int:
        return sum(r.tops_failed for r in self.report_stats)

    @property
    def tops_no_data(self) -> int:
        return sum(r.tops_no_data for r in self.report_stats)

    def print_summary(self):
        """Print detailed summary for this stakeholder."""
        typer.secho(f"\n{'='*80}", fg="cyan")
        typer.secho(
            f"📊 STAKEHOLDER SUMMARY: {self.stakeholder_name} ({self.stakeholder_type})",
            fg="cyan",
            bold=True,
        )
        typer.secho(f"{'='*80}", fg="cyan")

        # Overall stats
        typer.secho(f"\n📁 Reports: {self.total_reports}", fg="white")

        # KPI Stats
        if self.total_kpis > 0:
            kpi_success_rate = (self.kpis_successful / self.total_kpis) * 100
            typer.secho("\n📈 KPI Statistics:", fg="yellow", bold=True)
            typer.secho(f"  Total KPIs:      {self.total_kpis}", fg="white")
            typer.secho(
                f"  ✅ Successful:   {self.kpis_successful} ({kpi_success_rate:.1f}%)",
                fg="green",
            )
            typer.secho(f"  ❌ Failed:       {self.kpis_failed}", fg="red")
            typer.secho(f"  ⚠️  No Data:      {self.kpis_no_data}", fg="yellow")

        # Top Stats
        if self.total_tops > 0:
            top_success_rate = (self.tops_successful / self.total_tops) * 100
            typer.secho("\n🏆 Top Rankings Statistics:", fg="yellow", bold=True)
            typer.secho(f"  Total Tops:      {self.total_tops}", fg="white")
            typer.secho(
                f"  ✅ Successful:   {self.tops_successful} ({top_success_rate:.1f}%)",
                fg="green",
            )
            typer.secho(f"  ❌ Failed:       {self.tops_failed}", fg="red")
            typer.secho(f"  ⚠️  No Data:      {self.tops_no_data}", fg="yellow")

        # Per-report breakdown
        if len(self.report_stats) > 1:  # Only show breakdown if multiple reports
            typer.secho("\n📄 Per-Report Breakdown:", fg="yellow", bold=True)
            for report in self.report_stats:
                typer.secho(f"  • {report.report_name}:", fg="cyan")
                if report.total_kpis > 0:
                    typer.secho(
                        f"    KPIs: {report.kpis_successful}/{report.total_kpis} successful",
                        fg="white",
                    )
                if report.total_tops > 0:
                    typer.secho(
                        f"    Tops: {report.tops_successful}/{report.total_tops} successful",
                        fg="white",
                    )


@dataclass
class GlobalStats:
    """Global statistics across all stakeholders."""

    stakeholder_stats: List[StakeholderStats] = field(default_factory=list)

    def add_stakeholder_stats(self, stats: StakeholderStats):
        """Add stakeholder statistics."""
        self.stakeholder_stats.append(stats)

    @property
    def total_stakeholders(self) -> int:
        return len(self.stakeholder_stats)

    @property
    def total_reports(self) -> int:
        return sum(s.total_reports for s in self.stakeholder_stats)

    @property
    def total_kpis(self) -> int:
        return sum(s.total_kpis for s in self.stakeholder_stats)

    @property
    def kpis_successful(self) -> int:
        return sum(s.kpis_successful for s in self.stakeholder_stats)

    @property
    def kpis_failed(self) -> int:
        return sum(s.kpis_failed for s in self.stakeholder_stats)

    @property
    def kpis_no_data(self) -> int:
        return sum(s.kpis_no_data for s in self.stakeholder_stats)

    @property
    def total_tops(self) -> int:
        return sum(s.total_tops for s in self.stakeholder_stats)

    @property
    def tops_successful(self) -> int:
        return sum(s.tops_successful for s in self.stakeholder_stats)

    @property
    def tops_failed(self) -> int:
        return sum(s.tops_failed for s in self.stakeholder_stats)

    @property
    def tops_no_data(self) -> int:
        return sum(s.tops_no_data for s in self.stakeholder_stats)

    def print_detailed_summary(self):
        """Print comprehensive summary of all processing."""
        typer.secho(f"\n{'='*80}", fg="magenta")
        typer.secho("🎯 GLOBAL EXECUTION SUMMARY", fg="magenta", bold=True)
        typer.secho(f"{'='*80}", fg="magenta")

        # High-level stats
        typer.secho("\n📊 Overview:", fg="yellow", bold=True)
        typer.secho(f"  Stakeholders: {self.total_stakeholders}", fg="white")
        typer.secho(f"  Total Reports: {self.total_reports}", fg="white")

        # KPI Summary
        if self.total_kpis > 0:
            kpi_success_rate = (self.kpis_successful / self.total_kpis) * 100
            typer.secho("\n📈 Overall KPI Statistics:", fg="yellow", bold=True)
            typer.secho(f"  Total KPIs Processed:  {self.total_kpis}", fg="white")
            typer.secho(
                f"  ✅ Successful:         {self.kpis_successful} ({kpi_success_rate:.1f}%)",
                fg="green",
            )
            typer.secho(f"  ❌ Failed:             {self.kpis_failed}", fg="red")
            typer.secho(f"  ⚠️  No Data/Missing:    {self.kpis_no_data}", fg="yellow")

        # Top Summary
        if self.total_tops > 0:
            top_success_rate = (self.tops_successful / self.total_tops) * 100
            typer.secho("\n🏆 Overall Top Rankings Statistics:", fg="yellow", bold=True)
            typer.secho(f"  Total Tops Processed:  {self.total_tops}", fg="white")
            typer.secho(
                f"  ✅ Successful:         {self.tops_successful} ({top_success_rate:.1f}%)",
                fg="green",
            )
            typer.secho(f"  ❌ Failed:             {self.tops_failed}", fg="red")
            typer.secho(f"  ⚠️  No Data/Missing:    {self.tops_no_data}", fg="yellow")

        # NEW: Condensed per-stakeholder execution summary
        typer.secho(f"\n{'='*80}", fg="cyan")
        typer.secho("📊 DETAILED EXECUTION SUMMARY", fg="cyan", bold=True)
        typer.secho(f"{'='*80}", fg="cyan")

        for stakeholder in self.stakeholder_stats:
            typer.secho(
                f"\n🏢 {stakeholder.stakeholder_name} ({stakeholder.stakeholder_type})",
                fg="white",
                bold=True,
            )
            typer.secho(f"  Reports: {stakeholder.total_reports}", fg="white")

            if stakeholder.total_kpis > 0:
                kpi_success_rate = (
                    stakeholder.kpis_successful / stakeholder.total_kpis
                ) * 100
                typer.secho(
                    f"  Total KPIs Processed:  {stakeholder.total_kpis}", fg="white"
                )
                typer.secho(
                    f"  ✅ Successful:         {stakeholder.kpis_successful} ({kpi_success_rate:.1f}%)",
                    fg="green",
                )
                typer.secho(
                    f"  ❌ Failed:             {stakeholder.kpis_failed}", fg="red"
                )
                typer.secho(
                    f"  ⚠️  No Data/Missing:    {stakeholder.kpis_no_data}", fg="yellow"
                )

            if stakeholder.total_tops > 0:
                top_success_rate = (
                    stakeholder.tops_successful / stakeholder.total_tops
                ) * 100
                typer.secho(
                    f"  Total Tops Processed:  {stakeholder.total_tops}", fg="white"
                )
                typer.secho(
                    f"  ✅ Successful:         {stakeholder.tops_successful} ({top_success_rate:.1f}%)",
                    fg="green",
                )
                typer.secho(
                    f"  ❌ Failed:             {stakeholder.tops_failed}", fg="red"
                )
                typer.secho(
                    f"  ⚠️  No Data/Missing:    {stakeholder.tops_no_data}", fg="yellow"
                )

        # Per-stakeholder detailed breakdown
        typer.secho(f"\n{'─'*80}", fg="cyan")
        typer.secho("📋 Per-Stakeholder Detailed Breakdown:", fg="cyan", bold=True)
        typer.secho(f"{'─'*80}", fg="cyan")

        for stakeholder in self.stakeholder_stats:
            stakeholder.print_summary()

        # Final summary
        typer.secho(f"\n{'='*80}", fg="magenta")
        if (
            self.kpis_successful == self.total_kpis
            and self.tops_successful == self.total_tops
        ):
            typer.secho(
                "✅ ALL PROCESSING COMPLETED SUCCESSFULLY!", fg="green", bold=True
            )
        elif self.kpis_failed > 0 or self.tops_failed > 0:
            typer.secho("⚠️  PROCESSING COMPLETED WITH ERRORS", fg="yellow", bold=True)
        else:
            typer.secho("✅ PROCESSING COMPLETED", fg="green", bold=True)
        typer.secho(f"{'='*80}\n", fg="magenta")

    def get_failed_kpis(self) -> List[KPIResult]:
        """Get all failed KPIs across all stakeholders."""
        failed = []
        for stakeholder in self.stakeholder_stats:
            for report in stakeholder.report_stats:
                for sheet in report.sheet_stats:
                    failed.extend(
                        [
                            r
                            for r in sheet.kpi_results
                            if r.status
                            in [
                                KPIStatus.FAILED,
                                KPIStatus.WRITE_FAILED,
                                KPIStatus.NO_DATA,
                            ]
                        ]
                    )
        return failed

    def print_failed_kpis_detail(self):
        """Print detailed information about failed KPIs."""
        failed = self.get_failed_kpis()
        if not failed:
            return

        typer.secho("\n🔍 DETAILED FAILURE ANALYSIS:", fg="red", bold=True)
        typer.secho(f"{'─'*80}", fg="red")

        for result in failed:
            status_emoji = "❌" if result.status == KPIStatus.FAILED else "⚠️"
            typer.secho(
                f"\n{status_emoji} KPI: {result.kpi_name}", fg="white", bold=True
            )
            typer.secho(f"  Sheet: {result.sheet_name}", fg="cyan")
            typer.secho(f"  Status: {result.status.value}", fg="yellow")
            if result.error_message:
                typer.secho(f"  Error: {result.error_message}", fg="red")
            if result.total_cells > 0:
                typer.secho(
                    f"  Values written: {result.values_written}/{result.total_cells} ({result.success_rate:.1f}%)",
                    fg="white",
                )
