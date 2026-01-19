from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional, TextIO

import typer


def _print_to_both(msg: str, file_handle: Optional[TextIO] = None, **style_kwargs):
    """Print to console with colors and to file without colors."""
    typer.secho(msg, **style_kwargs)
    if file_handle:
        file_handle.write(msg + "\n")


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

    def print_summary(self, file_handle: Optional[TextIO] = None):
        """Print detailed summary for this stakeholder."""
        _print_to_both(f"\n{'='*80}", file_handle, fg="cyan")
        _print_to_both(
            f"📊 STAKEHOLDER SUMMARY: {self.stakeholder_name} ({self.stakeholder_type})",
            file_handle,
            fg="cyan",
            bold=True,
        )
        _print_to_both(f"{'='*80}", file_handle, fg="cyan")

        # Overall stats
        _print_to_both(f"\n📁 Reports: {self.total_reports}", file_handle, fg="white")

        # KPI Stats
        if self.total_kpis > 0:
            kpi_success_rate = (self.kpis_successful / self.total_kpis) * 100
            _print_to_both("\n📈 KPI Statistics:", file_handle, fg="yellow", bold=True)
            _print_to_both(
                f"  Total KPIs:      {self.total_kpis}", file_handle, fg="white"
            )
            _print_to_both(
                f"  ✅ Successful:   {self.kpis_successful} ({kpi_success_rate:.1f}%)",
                file_handle,
                fg="green",
            )
            _print_to_both(
                f"  ❌ Failed:       {self.kpis_failed}", file_handle, fg="red"
            )
            _print_to_both(
                f"  ⚠️  No Data:      {self.kpis_no_data}", file_handle, fg="yellow"
            )

        # Top Stats
        if self.total_tops > 0:
            top_success_rate = (self.tops_successful / self.total_tops) * 100
            _print_to_both(
                "\n🏆 Top Rankings Statistics:", file_handle, fg="yellow", bold=True
            )
            _print_to_both(
                f"  Total Tops:      {self.total_tops}", file_handle, fg="white"
            )
            _print_to_both(
                f"  ✅ Successful:   {self.tops_successful} ({top_success_rate:.1f}%)",
                file_handle,
                fg="green",
            )
            _print_to_both(
                f"  ❌ Failed:       {self.tops_failed}", file_handle, fg="red"
            )
            _print_to_both(
                f"  ⚠️  No Data:      {self.tops_no_data}", file_handle, fg="yellow"
            )

        # Per-report breakdown
        if len(self.report_stats) > 1:  # Only show breakdown if multiple reports
            _print_to_both(
                "\n📄 Per-Report Breakdown:", file_handle, fg="yellow", bold=True
            )
            for report in self.report_stats:
                _print_to_both(f"  • {report.report_name}:", file_handle, fg="cyan")
                if report.total_kpis > 0:
                    _print_to_both(
                        f"    KPIs: {report.kpis_successful}/{report.total_kpis} successful",
                        file_handle,
                        fg="white",
                    )
                if report.total_tops > 0:
                    _print_to_both(
                        f"    Tops: {report.tops_successful}/{report.total_tops} successful",
                        file_handle,
                        fg="white",
                    )


@dataclass
class GlobalStats:
    """Global statistics across all stakeholders."""

    stakeholder_stats: List[StakeholderStats] = field(default_factory=list)

    def add_stakeholder_stats(self, stats: StakeholderStats):
        """Add stakeholder statistics."""
        self.stakeholder_stats.append(stats)

    def add_report_stats_to_stakeholder(
        self, stakeholder_name: str, stakeholder_type: str, report_stats: ReportStats
    ):
        """Add report stats to a specific stakeholder, creating the stakeholder stats if needed."""
        # Find existing
        for s in self.stakeholder_stats:
            if s.stakeholder_name == stakeholder_name:
                s.add_report_stats(report_stats)
                return

        # Create new
        new_s = StakeholderStats(stakeholder_name, stakeholder_type)
        new_s.add_report_stats(report_stats)
        self.stakeholder_stats.append(new_s)

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

    def print_detailed_summary(self, file_handle: Optional[TextIO] = None):
        """Print comprehensive summary of all processing."""
        _print_to_both(f"\n{'='*80}", file_handle, fg="magenta")
        _print_to_both(
            "🎯 GLOBAL EXECUTION SUMMARY", file_handle, fg="magenta", bold=True
        )
        _print_to_both(f"{'='*80}", file_handle, fg="magenta")

        # High-level stats
        _print_to_both("\n📊 Overview:", file_handle, fg="yellow", bold=True)
        _print_to_both(
            f"  Stakeholders: {self.total_stakeholders}", file_handle, fg="white"
        )
        _print_to_both(
            f"  Total Reports: {self.total_reports}", file_handle, fg="white"
        )

        # KPI Summary
        if self.total_kpis > 0:
            kpi_success_rate = (self.kpis_successful / self.total_kpis) * 100
            _print_to_both(
                "\n📈 Overall KPI Statistics:", file_handle, fg="yellow", bold=True
            )
            _print_to_both(
                f"  Total KPIs Processed:  {self.total_kpis}", file_handle, fg="white"
            )
            _print_to_both(
                f"  ✅ Successful:         {self.kpis_successful} ({kpi_success_rate:.1f}%)",
                file_handle,
                fg="green",
            )
            _print_to_both(
                f"  ❌ Failed:             {self.kpis_failed}", file_handle, fg="red"
            )
            _print_to_both(
                f"  ⚠️  No Data/Missing:    {self.kpis_no_data}",
                file_handle,
                fg="yellow",
            )

        # Top Summary
        if self.total_tops > 0:
            top_success_rate = (self.tops_successful / self.total_tops) * 100
            _print_to_both(
                "\n🏆 Overall Top Rankings Statistics:",
                file_handle,
                fg="yellow",
                bold=True,
            )
            _print_to_both(
                f"  Total Tops Processed:  {self.total_tops}", file_handle, fg="white"
            )
            _print_to_both(
                f"  ✅ Successful:         {self.tops_successful} ({top_success_rate:.1f}%)",
                file_handle,
                fg="green",
            )
            _print_to_both(
                f"  ❌ Failed:             {self.tops_failed}", file_handle, fg="red"
            )
            _print_to_both(
                f"  ⚠️  No Data/Missing:    {self.tops_no_data}",
                file_handle,
                fg="yellow",
            )

        # NEW: Condensed per-stakeholder execution summary
        _print_to_both(f"\n{'='*80}", file_handle, fg="cyan")
        _print_to_both(
            "📊 DETAILED EXECUTION SUMMARY", file_handle, fg="cyan", bold=True
        )
        _print_to_both(f"{'='*80}", file_handle, fg="cyan")

        for stakeholder in self.stakeholder_stats:
            _print_to_both(
                f"\n🏢 {stakeholder.stakeholder_name} ({stakeholder.stakeholder_type})",
                file_handle,
                fg="white",
                bold=True,
            )
            _print_to_both(
                f"  Reports: {stakeholder.total_reports}", file_handle, fg="white"
            )

            if stakeholder.total_kpis > 0:
                kpi_success_rate = (
                    stakeholder.kpis_successful / stakeholder.total_kpis
                ) * 100
                _print_to_both(
                    f"  Total KPIs Processed:  {stakeholder.total_kpis}",
                    file_handle,
                    fg="white",
                )
                _print_to_both(
                    f"  ✅ Successful:         {stakeholder.kpis_successful} ({kpi_success_rate:.1f}%)",
                    file_handle,
                    fg="green",
                )
                _print_to_both(
                    f"  ❌ Failed:             {stakeholder.kpis_failed}",
                    file_handle,
                    fg="red",
                )
                _print_to_both(
                    f"  ⚠️  No Data/Missing:    {stakeholder.kpis_no_data}",
                    file_handle,
                    fg="yellow",
                )

            if stakeholder.total_tops > 0:
                top_success_rate = (
                    stakeholder.tops_successful / stakeholder.total_tops
                ) * 100
                _print_to_both(
                    f"  Total Tops Processed:  {stakeholder.total_tops}",
                    file_handle,
                    fg="white",
                )
                _print_to_both(
                    f"  ✅ Successful:         {stakeholder.tops_successful} ({top_success_rate:.1f}%)",
                    file_handle,
                    fg="green",
                )
                _print_to_both(
                    f"  ❌ Failed:             {stakeholder.tops_failed}",
                    file_handle,
                    fg="red",
                )
                _print_to_both(
                    f"  ⚠️  No Data/Missing:    {stakeholder.tops_no_data}",
                    file_handle,
                    fg="yellow",
                )

        # Per-stakeholder detailed breakdown
        _print_to_both(f"\n{'─'*80}", file_handle, fg="cyan")
        _print_to_both(
            "📋 Per-Stakeholder Detailed Breakdown:", file_handle, fg="cyan", bold=True
        )
        _print_to_both(f"{'─'*80}", file_handle, fg="cyan")

        for stakeholder in self.stakeholder_stats:
            stakeholder.print_summary(file_handle)

        # Final summary
        _print_to_both(f"\n{'='*80}", file_handle, fg="magenta")
        if (
            self.kpis_successful == self.total_kpis
            and self.tops_successful == self.total_tops
        ):
            _print_to_both(
                "✅ ALL PROCESSING COMPLETED SUCCESSFULLY!",
                file_handle,
                fg="green",
                bold=True,
            )
        elif self.kpis_failed > 0 or self.tops_failed > 0:
            _print_to_both(
                "⚠️  PROCESSING COMPLETED WITH ERRORS",
                file_handle,
                fg="yellow",
                bold=True,
            )
        else:
            _print_to_both(
                "✅ PROCESSING COMPLETED", file_handle, fg="green", bold=True
            )
        _print_to_both(f"{'='*80}\n", file_handle, fg="magenta")

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

    def print_failed_kpis_detail(self, file_handle: Optional[TextIO] = None):
        """Print detailed information about failed KPIs."""
        failed = self.get_failed_kpis()
        if not failed:
            return

        _print_to_both(
            "\n🔍 DETAILED FAILURE ANALYSIS:", file_handle, fg="red", bold=True
        )
        _print_to_both(f"{'─'*80}", file_handle, fg="red")

        for result in failed:
            status_emoji = "❌" if result.status == KPIStatus.FAILED else "⚠️"
            _print_to_both(
                f"\n{status_emoji} KPI: {result.kpi_name}",
                file_handle,
                fg="white",
                bold=True,
            )
            _print_to_both(f"  Sheet: {result.sheet_name}", file_handle, fg="cyan")
            _print_to_both(f"  Status: {result.status.value}", file_handle, fg="yellow")
            if result.error_message:
                _print_to_both(
                    f"  Error: {result.error_message}", file_handle, fg="red"
                )
            if result.total_cells > 0:
                _print_to_both(
                    f"  Values written: {result.values_written}/{result.total_cells} ({result.success_rate:.1f}%)",
                    file_handle,
                    fg="white",
                )

    def save_to_file(self, filepath: Path, show_failures: bool = False):
        """Save statistics to a file."""
        with open(filepath, "w", encoding="utf-8") as f:
            self.print_detailed_summary(file_handle=f)
            if show_failures:
                self.print_failed_kpis_detail(file_handle=f)
