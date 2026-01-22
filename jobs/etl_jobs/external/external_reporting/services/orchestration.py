from typing import Any, Dict, List, Optional

from duckdb import DuckDBPyConnection

from config import (
    AGG_TYPE_MAPPING,
    DEFAULT_AGG_TYPE,
    SHEET_DEFINITIONS,
    SHEET_LAYOUT,
    SOURCE_TABLES,
)
from services.data import DataService
from services.excel_layout import ExcelLayoutService
from services.excel_writer import ExcelWriterService
from services.tracking import KPIResult, KPIStatus, SheetStats, TopResult, TopStatus
from utils.verbose_logger import log_print


class ReportOrchestrationService:
    """Orchestrates the complete report generation process with error handling."""

    def __init__(self, duckdb_conn: DuckDBPyConnection, fetcher_concurrency: int = 2):
        self.conn = duckdb_conn
        self.data_service = DataService(duckdb_conn)
        self.fetcher_concurrency = fetcher_concurrency

    def _get_layout_type(self, sheet) -> str:
        """Determine layout type based on sheet definition."""
        if sheet.definition.startswith("top"):
            return "top"
        elif sheet.definition.endswith("kpis"):
            return "kpis"
        else:
            return "other"

    def process_all_sheets(
        self, sheets: List, ds: str, context: Optional[Dict[str, Any]] = None
    ) -> List[SheetStats]:
        """
        Process all sheets in a report with comprehensive error handling.

        Args:
            sheets: List of Sheet objects from Report
            ds: Consolidation date in YYYY-MM-DD format
            context: Optional context dictionary (e.g. for logging report name)

        Returns:
            List of SheetStats with detailed results
        """
        all_sheet_stats = []
        context = context or {}
        report_name = context.get("report_name", "Unknown Report")

        log_print.info(f"➡️  Processing {len(sheets)} sheets for {report_name}")

        for sheet in sheets:
            try:
                sheet_stats = self._process_single_sheet(sheet, ds, context)
                all_sheet_stats.append(sheet_stats)

            except Exception as e:
                log_print.warning(
                    f"[{report_name}] Unexpected error processing sheet {sheet.tab_name}: {e}"
                )
                # Create failed sheet stats
                sheet_stats = SheetStats(
                    sheet_name=sheet.tab_name, sheet_type=sheet.definition
                )
                all_sheet_stats.append(sheet_stats)

        # Summary logging
        total_sheets = len(all_sheet_stats)
        successful_sheets = sum(
            1
            for s in all_sheet_stats
            if (
                s.kpis_successful > 0
                or s.tops_successful > 0
                or (len(s.kpi_results) == 0 and len(s.top_results) == 0)
            )
        )

        log_print.info(
            f"✅ Processing complete for {report_name}: {successful_sheets}/{total_sheets} sheets successful"
        )

        return all_sheet_stats

    def _process_single_sheet(
        self, sheet, ds: str, context: Optional[Dict[str, Any]] = None
    ) -> SheetStats:
        """
        Process a single sheet with error recovery.

        Args:
            sheet: Sheet object with worksheet, definition, context, filters
            ds: Consolidation date
            context: Optional context dictionary

        Returns:
            SheetStats with detailed results
        """
        sheet_stats = SheetStats(sheet_name=sheet.tab_name, sheet_type=sheet.definition)
        context = context or {}
        report_name = context.get("report_name", "Unknown Report")

        try:
            log_print.debug(f"[{report_name}] 📊 Processing sheet: {sheet.tab_name}")

            # Step 1: Layout preprocessing (date column expansion)
            expansion_result = self._handle_layout_preprocessing(sheet, ds)
            if not expansion_result and sheet.definition.endswith("kpis"):
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] Failed to expand date columns"
                )
                return sheet_stats

            # Extract date_mappings from expansion_result
            date_mappings = (
                expansion_result.get("date_mappings") if expansion_result else {}
            )

            # Step 2: Fill data based on sheet type
            if sheet.definition in ("individual_kpis", "collective_kpis"):
                self._handle_kpi_data_filling(
                    sheet, ds, date_mappings, sheet_stats, context
                )
            elif sheet.definition.startswith("top"):
                self._handle_top_data_filling(sheet, ds, sheet_stats, context)
            elif sheet.definition == "lexique":
                # Lexique sheets need no data processing
                pass
            else:
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] Unknown sheet definition: {sheet.definition}"
                )

            # Log completion
            if sheet.definition in ("individual_kpis", "collective_kpis"):
                log_print.debug(
                    f"[{report_name}] ✅ Completed sheet {sheet.tab_name}: "
                    f"{sheet_stats.kpis_successful} KPIs successful, "
                    f"{sheet_stats.kpis_failed} KPIs failed, "
                    f"{sheet_stats.kpis_no_data} KPIs with no data"
                )
            elif sheet.definition.startswith("top"):
                log_print.debug(
                    f"[{report_name}] ✅ Completed sheet {sheet.tab_name}: "
                    f"{sheet_stats.tops_successful} tops successful, "
                    f"{sheet_stats.tops_failed} tops failed"
                )
            elif sheet.definition == "lexique":
                log_print.debug(f"[{report_name}] ✅ Completed sheet {sheet.tab_name}")

            # Step 3: Delete template columns and Set title
            layout_type = self._get_layout_type(sheet)
            ExcelLayoutService.cleanup_template_columns(sheet.worksheet, layout_type)
            self._handle_title_setting(sheet, expansion_result)
            ExcelLayoutService.freeze_panes(sheet.worksheet, layout_type)

            return sheet_stats

        except Exception as e:
            log_print.warning(
                f"[{report_name}] Failed to process sheet {sheet.tab_name}: {e}"
            )
            return sheet_stats

    def _handle_layout_preprocessing(self, sheet, ds: str) -> Optional[Dict[str, Any]]:
        """Handle Excel layout preprocessing (date column expansion)."""
        try:
            if sheet.definition.endswith("kpis"):
                expansion_result = ExcelLayoutService.expand_date_columns_kpis(
                    worksheet=sheet.worksheet, sheet_definition=sheet.definition, ds=ds
                )
                return expansion_result
            return {}  # No date mappings needed for non-KPI sheets

        except Exception as e:
            log_print.warning(f"Layout preprocessing failed for {sheet.tab_name}: {e}")
            return None

    def _handle_title_setting(self, sheet, expansion_result: Dict[str, Any] = None):
        """Handle sheet title setting."""
        try:
            # Determine layout type
            layout_type = self._get_layout_type(sheet)

            expanded_width = None
            if layout_type == "kpis" and expansion_result:
                expanded_width = expansion_result.get("total_width")
            ExcelLayoutService.set_sheet_title(
                worksheet=sheet.worksheet,
                title_base=sheet.tab_name or sheet.definition.capitalize(),
                layout_type=layout_type,
                context=sheet.context or {},
                filters=sheet.filters or {},
                expanded_width=expanded_width,
                sheet_definition=sheet.definition,
            )

        except Exception as e:
            log_print.warning(f"Failed to set title for {sheet.tab_name}: {e}")

    def _handle_top_data_filling(
        self,
        sheet,
        ds: str,
        sheet_stats: SheetStats,
        context: Optional[Dict[str, Any]] = None,
    ):
        """Handle top data filling for top sheets."""
        context = context or {}
        report_name = context.get("report_name", "Unknown Report")
        try:
            source_table_key = SHEET_DEFINITIONS[sheet.definition].get("source_table")
            if not source_table_key or source_table_key not in SOURCE_TABLES:
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] No source table found for sheet definition: {sheet.definition}"
                )
                top_result = TopResult(
                    top_name=sheet.definition,
                    status=TopStatus.FAILED,
                    error_message="No source table configured",
                )
                sheet_stats.add_top_result(top_result)
                return

            table_config = SOURCE_TABLES[source_table_key]
            table_name = table_config["table"]

            dimension_context = sheet.get_dimension_context()
            if not dimension_context:
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] Could not resolve dimension context"
                )
                top_result = TopResult(
                    top_name=sheet.definition,
                    status=TopStatus.FAILED,
                    error_message="Could not resolve dimension context",
                )
                sheet_stats.add_top_result(top_result)
                return

            min_row = (
                SHEET_LAYOUT["top"]["title_row_offset"]
                + SHEET_LAYOUT["top"]["title_height"]
                + 2
            )

            top_data = self.data_service.get_top_rankings(
                dimension_name=dimension_context["name"],
                dimension_value=dimension_context["value"],
                ds=ds,
                table_name=table_name,
                top_n=SHEET_DEFINITIONS[sheet.definition].get("top_n", 50),
                select_fields=SHEET_DEFINITIONS[sheet.definition].get(
                    "select_fields", []
                ),
                ranking=SHEET_DEFINITIONS[sheet.definition].get("ranking", {}),
            )

            if top_data is not None and len(top_data) > 0:
                # Write data to Excel
                write_success = ExcelWriterService.write_top_data_to_sheet(
                    worksheet=sheet.worksheet, top_data=top_data, start_row=min_row
                )

                if write_success:
                    top_result = TopResult(
                        top_name=sheet.definition,
                        status=TopStatus.SUCCESS,
                        rows_written=len(top_data),
                    )
                else:
                    top_result = TopResult(
                        top_name=sheet.definition,
                        status=TopStatus.FAILED,
                        error_message="Failed to write data to Excel",
                    )
                sheet_stats.add_top_result(top_result)
            else:
                top_result = TopResult(
                    top_name=sheet.definition,
                    status=TopStatus.NO_DATA,
                    error_message="No data returned from query",
                )
                sheet_stats.add_top_result(top_result)

        except Exception as e:
            log_print.warning(
                f"[{report_name}] [{sheet.tab_name}] Top data filling failed: {e}"
            )
            top_result = TopResult(
                top_name=sheet.definition, status=TopStatus.FAILED, error_message=str(e)
            )
            sheet_stats.add_top_result(top_result)

    def _handle_kpi_data_filling(
        self,
        sheet,
        ds: str,
        date_mappings: Dict[str, Any],
        sheet_stats: SheetStats,
        context: Optional[Dict[str, Any]] = None,
    ):
        """Handle KPI data filling for KPI sheets using multithreading for data fetching."""
        import concurrent.futures

        context = context or {}
        report_name = context.get("report_name", "Unknown Report")

        try:
            # Get data source table
            source_table_key = SHEET_DEFINITIONS[sheet.definition].get("source_table")
            if not source_table_key or source_table_key not in SOURCE_TABLES:
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] No source table found for sheet definition: {sheet.definition}"
                )
                return

            table_config = SOURCE_TABLES[source_table_key]
            table_name = table_config["table"]
            scope = (
                "individual" if sheet.definition == "individual_kpis" else "collective"
            )

            # Get dimension context
            dimension_context = sheet.get_dimension_context()
            if not dimension_context:
                log_print.warning(
                    f"[{report_name}] [{sheet.tab_name}] Could not resolve dimension context"
                )
                return

            # Process each KPI row
            min_row = (
                SHEET_LAYOUT["kpis"]["title_row_offset"]
                + SHEET_LAYOUT["kpis"]["title_height"]
                + 2
            )

            # Calculate total cells for tracking
            total_years = len(date_mappings.get("years", []))
            total_months = len(date_mappings.get("months", []))
            total_cells_per_kpi = total_years + total_months

            # Phase 1: Parsing - Identify all KPI tasks
            kpi_tasks = []
            for row_idx, row in enumerate(
                sheet.worksheet.iter_rows(
                    min_row=min_row, max_row=sheet.worksheet.max_row
                )
            ):
                kpi_config = self._parse_kpi_row(row, row_idx + min_row - 1)
                if kpi_config:
                    kpi_tasks.append(kpi_config)

            if not kpi_tasks:
                return

            log_print.debug(
                f"[{report_name}] ⚡ Fetching {len(kpi_tasks)} KPIs concurrently for {sheet.tab_name}..."
            )

            # Phase 2: Concurrent Fetching
            # We use self.fetcher_concurrency (configured from main.py)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.fetcher_concurrency
            ) as executor:
                # Submit all tasks
                future_to_config = {
                    executor.submit(
                        self.data_service.get_kpi_data,
                        kpi_name=config["kpi_name"],
                        dimension_name=dimension_context["name"],
                        dimension_value=dimension_context["value"],
                        ds=ds,
                        scope=scope,
                        table_name=table_name,
                        select_field=config["select_field"],
                        agg_type=config["agg_type"],
                        context=context,
                    ): config
                    for config in kpi_tasks
                }

                # Phase 3: Sequential Writing (as results arrive)
                for future in concurrent.futures.as_completed(future_to_config):
                    kpi_config = future_to_config[future]
                    kpi_name = kpi_config["kpi_name"]
                    writting_fail_count = 0
                    no_data_count = 0

                    try:
                        kpi_data = future.result()

                        has_data = kpi_data and (
                            kpi_data.get("yearly") or kpi_data.get("monthly")
                        )

                        if has_data:
                            # Write data to Excel (Main thread / Synchronized by virtue of loop)
                            write_success = ExcelWriterService.write_kpi_data_to_sheet(
                                worksheet=sheet.worksheet,
                                kpi_data=kpi_data,
                                date_mappings=date_mappings,
                                row_idx=kpi_config["row_idx"],
                            )

                            if write_success:
                                # Count how many values were actually written
                                values_written = 0
                                yearly_data = kpi_data.get("yearly", {})
                                monthly_data = kpi_data.get("monthly", {})
                                values_written = len(yearly_data) + len(monthly_data)

                                kpi_result = KPIResult(
                                    kpi_name=kpi_name,
                                    status=KPIStatus.SUCCESS,
                                    values_written=values_written,
                                    total_cells=total_cells_per_kpi,
                                )
                            else:
                                kpi_result = KPIResult(
                                    kpi_name=kpi_name,
                                    status=KPIStatus.WRITE_FAILED,
                                    values_written=0,
                                    total_cells=total_cells_per_kpi,
                                    error_message="Failed to write to Excel",
                                )
                                writting_fail_count += 1

                            sheet_stats.add_kpi_result(kpi_result)

                        else:
                            kpi_result = KPIResult(
                                kpi_name=kpi_name,
                                status=KPIStatus.NO_DATA,
                                values_written=0,
                                total_cells=total_cells_per_kpi,
                                error_message="No data returned from query",
                            )
                            sheet_stats.add_kpi_result(kpi_result)
                            no_data_count += 1

                    except Exception as exc:
                        log_print.warning(
                            f"[{report_name}] [{sheet.tab_name}] Failed to fetch data for KPI '{kpi_name}': {exc}"
                        )
                        kpi_result = KPIResult(
                            kpi_name=kpi_name,
                            status=KPIStatus.FAILED,
                            error_message=str(exc),
                        )
                        sheet_stats.add_kpi_result(kpi_result)

                    if writting_fail_count + no_data_count > 0:
                        log_print.debug(
                            f"[{report_name}] KPI issue for '{kpi_name}' in {sheet.tab_name}: "
                            f"{writting_fail_count} write failures, {no_data_count} no data"
                        )

        except Exception as e:
            log_print.warning(
                f"[{report_name}] [{sheet.tab_name}] KPI data filling failed: {e}"
            )

    def _parse_kpi_row(self, row, row_idx: int) -> Optional[Dict[str, Any]]:
        """
        Parse a KPI row configuration from Excel cells.

        Args:
            row: Excel row from openpyxl
            row_idx: Row index (0-based)

        Returns:
            KPI configuration dict or None if parsing failed
        """
        try:
            # Parse filter cell (expected format: "kpi_name=actual_name")
            filter_cell = row[0].value if len(row) > 0 else None
            if not filter_cell or "=" not in str(filter_cell):
                return None

            kpi_name = str(filter_cell).split("=")[1].strip()
            if not kpi_name:
                return None

            # Parse select field (default: kpi)
            select_field = "kpi"
            if len(row) > 1 and row[1].value:
                select_field = str(row[1].value).strip().lower()

            # Parse aggregation type (default: sum)
            excel_agg_type = DEFAULT_AGG_TYPE
            if len(row) > 2 and row[2].value:
                excel_agg_type = str(row[2].value).strip().lower()

            # Map Excel value to technical value
            mapped_agg_type = self._map_aggregation_type(excel_agg_type)

            return {
                "kpi_name": kpi_name,
                "select_field": select_field,
                "agg_type": mapped_agg_type,
                "row_idx": row_idx,
            }

        except Exception as e:
            log_print.debug(f"Failed to parse KPI row at index {row_idx}: {e}")
            return None

    def _map_aggregation_type(self, excel_agg_type: str) -> str:
        """Map Excel aggregation type to technical aggregation type."""
        if not excel_agg_type:
            return DEFAULT_AGG_TYPE

        # Clean the input (lowercase, strip spaces)
        clean_agg_type = excel_agg_type.lower().strip()

        # Look up in mapping
        LOWER_AGG_TYPE_MAPPING = {
            k.lower().strip(): v for k, v in AGG_TYPE_MAPPING.items()
        }
        technical_agg_type = LOWER_AGG_TYPE_MAPPING.get(
            clean_agg_type, DEFAULT_AGG_TYPE
        )
        if technical_agg_type != clean_agg_type:
            log_print.debug(
                f"Mapped aggregation type: '{excel_agg_type}' -> '{technical_agg_type}'"
            )

        return technical_agg_type
