from typing import Dict, List, Any, Optional
from pathlib import Path
from duckdb import DuckDBPyConnection
import logging
import typer

from services.data import DataService
from services.excel_layout import ExcelLayoutService
from services.excel_writer import ExcelWriterService

from config import SHEET_DEFINITIONS, SOURCE_TABLES, AGG_TYPE_MAPPING, DEFAULT_AGG_TYPE, SHEET_LAYOUT


logger = logging.getLogger(__name__)


class ReportOrchestrationService:
    """Orchestrates the complete report generation process with error handling."""
    
    def __init__(self, duckdb_conn: DuckDBPyConnection):
        self.conn = duckdb_conn
        self.data_service = DataService(duckdb_conn)
        
    def process_all_sheets(self, sheets: List, ds: str) -> Dict[str, Any]:
        """
        Process all sheets in a report with comprehensive error handling.
        
        Args:
            sheets: List of Sheet objects from Report
            ds: Consolidation date in YYYY-MM-DD format
            
        Returns:
            Processing statistics dict
        """
        stats = {
            "sheets_processed": 0,
            "sheets_successful": 0,
            "sheets_failed": 0,
            "kpis_successful": 0,
            "kpis_failed": 0
        }
        
        typer.secho(f"➡️ Processing {len(sheets)} sheets", fg="cyan")
        
        for sheet in sheets:
            try:
                sheet_success = self._process_single_sheet(sheet, ds)
                stats["sheets_processed"] += 1
                
                if sheet_success["success"]:
                    stats["sheets_successful"] += 1
                    stats["kpis_successful"] += sheet_success.get("kpis_successful", 0)
                else:
                    stats["sheets_failed"] += 1
                    
                stats["kpis_failed"] += sheet_success.get("kpis_failed", 0)
                
            except Exception as e:
                logger.warning(f"Unexpected error processing sheet {sheet.tab_name}: {e}")
                stats["sheets_processed"] += 1
                stats["sheets_failed"] += 1
        
        # Final report logging
        success_rate = (stats["sheets_successful"] / stats["sheets_processed"]) * 100 if stats["sheets_processed"] > 0 else 0
        typer.secho(
            f"✅ Processing complete: {stats['sheets_successful']}/{stats['sheets_processed']} sheets successful ({success_rate:.1f}%)",
            fg="green"
        )
        
        if stats["kpis_failed"] > 0:
            typer.secho(f"⚠️  {stats['kpis_failed']} KPIs failed to process", fg="yellow")
            
        return stats
    
    def _process_single_sheet(self, sheet, ds: str) -> Dict[str, Any]:
        """
        Process a single sheet with error recovery.
        
        Args:
            sheet: Sheet object with worksheet, definition, context, filters
            ds: Consolidation date
            
        Returns:
            Dict with processing results and statistics
        """
        result = {
            "success": False,
            "kpis_successful": 0,
            "kpis_failed": 0
        }
        
        try:
            typer.echo(f"📊 Processing sheet: {sheet.tab_name}")
            
            # Step 1: Layout preprocessing (date column expansion)
            expansion_result = self._handle_layout_preprocessing(sheet, ds)
            if not expansion_result and sheet.definition.endswith("kpis"):
                logger.warning(f"Failed to expand date columns for {sheet.tab_name}")
                return result
            
            # Extract date_mappings from expansion_result
            date_mappings = expansion_result.get("date_mappings") if expansion_result else {}
            
            # Step 2: Set title (pass the full expansion_result for width calculation)
            layout_type = "top" if sheet.definition.startswith("top") else "kpis" if sheet.definition.endswith("kpis") else "other"
            ExcelLayoutService.cleanup_template_columns(sheet.worksheet, layout_type)
            
            self._handle_title_setting(sheet, expansion_result)
            
            # Step 3: Fill data based on sheet type
            if sheet.definition in ("individual_kpis", "collective_kpis"):
                kpi_result = self._handle_kpi_data_filling(sheet, ds, date_mappings)  # Use extracted date_mappings
                result.update(kpi_result)
            elif sheet.definition.startswith("top"):
                top_result = self._handle_top_data_filling(sheet, ds)
                result.update(top_result)
                result["success"] = True
            elif sheet.definition == "lexique":
                # Lexique sheets need no data processing
                result["success"] = True
            else:
                logger.warning(f"Unknown sheet definition: {sheet.definition}")
                        
        
            if sheet.definition in ("individual_kpis", "collective_kpis"):
                n_success = result.get("kpis_successful", 0)
                n_failed = result.get("kpis_failed", 0)
                typer.echo(f"✅ Completed sheet {sheet.tab_name}: {n_success} KPIs successful, {n_failed} KPIs failed")
            elif sheet.definition.startswith("top"):    
                n_success = result.get("tops_successful", 0)
                n_failed = result.get("tops_failed", 0)
                typer.echo(f"✅ Completed sheet {sheet.tab_name}: {n_success} tops successful, {n_failed} tops failed")
            elif sheet.definition == "lexique":
                typer.echo(f"✅ Completed sheet {sheet.tab_name}")
                
            return result
            
        except Exception as e:
            logger.warning(f"Failed to process sheet {sheet.tab_name}: {e}")
            return result
    
    def _handle_layout_preprocessing(self, sheet, ds: str) -> Optional[Dict[str, Any]]:
        """Handle Excel layout preprocessing (date column expansion)."""
        try:
            if sheet.definition.endswith("kpis"):
                expansion_result = ExcelLayoutService.expand_date_columns_kpis(
                    worksheet=sheet.worksheet,
                    sheet_definition=sheet.definition,
                    ds=ds
                )
                return expansion_result
            return {}  # No date mappings needed for non-KPI sheets
            
        except Exception as e:
            logger.warning(f"Layout preprocessing failed for {sheet.tab_name}: {e}")
            return None
    
    def _handle_title_setting(self, sheet,expansion_result: Dict[str, Any] = None):
        """Handle sheet title setting."""
        try:
            # Determine layout type
            layout_type = "top" if sheet.definition.startswith("top") else "kpis" if sheet.definition.endswith("kpis") else "other"
            
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
                sheet_definition=sheet.definition
                
            )
            
        except Exception as e:
            logger.warning(f"Failed to set title for {sheet.tab_name}: {e}")

    def _handle_top_data_filling(self, sheet, ds: str) -> Dict[str, Any]:
        """Handle top data filling for top sheets."""
        result = {"success": True, "tops_successful": 0, "tops_failed": 0}
        try:
            source_table_key = SHEET_DEFINITIONS[sheet.definition].get("source_table")
            if not source_table_key or source_table_key not in SOURCE_TABLES:
                logger.warning(f"No source table found for sheet definition: {sheet.definition}")
                result["success"] = False
                return result
            
            
            table_config = SOURCE_TABLES[source_table_key]
            table_name = table_config["table"]

            dimension_context = sheet.get_dimension_context()
            if not dimension_context:
                logger.warning(f"Could not resolve dimension context for {sheet.tab_name}")
                result["success"] = False
                return result

            min_row = SHEET_LAYOUT["top"]["title_row_offset"] + SHEET_LAYOUT["top"]["title_height"] + 2
            
            top_data = self.data_service.get_top_rankings(
                dimension_name=dimension_context["name"],
                dimension_value=dimension_context["value"],
                ds=ds,
                table_name=table_name,
                top_n=SHEET_DEFINITIONS[sheet.definition].get("top_n", 50),
                select_fields=SHEET_DEFINITIONS[sheet.definition].get("select_fields", []),
                order_by=SHEET_DEFINITIONS[sheet.definition].get("order_by", []),
                
            )
            # print(f"DEBUG: top_data type: {type(top_data)}")
            # print(f"DEBUG: top_data shape: {top_data.shape if hasattr(top_data, 'shape') else 'N/A'}")
            # print(f"DEBUG: top_data empty?: {top_data.empty if hasattr(top_data, 'empty') else 'N/A'}")
            # print(f"DEBUG: min_row: {min_row}")
            
            if len(top_data) > 0:
                    # Write data to Excel
                    write_success = ExcelWriterService.write_top_data_to_sheet(
                        worksheet=sheet.worksheet,
                        top_data=top_data,
                        start_row=min_row
                    )
                    
                    if write_success:
                        result["tops_successful"] += 1
                    else:
                        result["tops_failed"] += 1
            else:
                result["tops_failed"] += 1
            
            return result
            
        except Exception as e:
            logger.warning(f"Top data filling failed for {sheet.tab_name}: {e}")
            result["success"] = False
            return result
    
    
    def _handle_kpi_data_filling(
        self, 
        sheet, 
        ds: str, 
        date_mappings: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle KPI data filling for KPI sheets."""
        result = {"success": True, "kpis_successful": 0, "kpis_failed": 0}
        
        try:
            # Get data source table
            source_table_key = SHEET_DEFINITIONS[sheet.definition].get("source_table")
            if not source_table_key or source_table_key not in SOURCE_TABLES:
                logger.warning(f"No source table found for sheet definition: {sheet.definition}")
                result["success"] = False
                return result
                
            table_config = SOURCE_TABLES[source_table_key]
            table_name = table_config["table"]
            scope = "individual" if sheet.definition == "individual_kpis" else "collective"
            
            # Get dimension context  
            dimension_context = sheet.get_dimension_context()
            if not dimension_context:
                logger.warning(f"Could not resolve dimension context for {sheet.tab_name}")
                result["success"] = False
                return result
            
            # Process each KPI row
            min_row = SHEET_LAYOUT["kpis"]["title_row_offset"] + SHEET_LAYOUT["kpis"]["title_height"] + 2
            for row_idx, row in enumerate(sheet.worksheet.iter_rows(min_row=min_row, max_row=sheet.worksheet.max_row)):
                kpi_config = self._parse_kpi_row(row, row_idx + min_row - 1)
                if not kpi_config:
                    continue
                    
                # Get KPI data
                kpi_data = self.data_service.get_kpi_data(
                    kpi_name=kpi_config["kpi_name"],
                    dimension_name=dimension_context["name"],
                    dimension_value=dimension_context["value"],
                    ds=ds,
                    scope=scope,
                    table_name=table_name,
                    select_field=kpi_config["select_field"],
                    agg_type=kpi_config["agg_type"]
                )
                
                if kpi_data:
                    # Write data to Excel
                    write_success = ExcelWriterService.write_kpi_data_to_sheet(
                        worksheet=sheet.worksheet,
                        kpi_data=kpi_data,
                        date_mappings=date_mappings,
                        row_idx=kpi_config["row_idx"]
                    )
                    
                    if write_success:
                        result["kpis_successful"] += 1
                    else:
                        result["kpis_failed"] += 1
                        logger.warning(f"Failed to write KPI '{kpi_config['kpi_name']}' to sheet {sheet.tab_name}")
                else:
                    result["kpis_failed"] += 1
                    logger.warning(f"No data found for KPI '{kpi_config['kpi_name']}' in sheet {sheet.tab_name}")
            
            return result
            
        except Exception as e:
            logger.warning(f"KPI data filling failed for {sheet.tab_name}: {e}")
            result["success"] = False
            return result
    
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
                "row_idx": row_idx
            }
            
        except Exception as e:
            logger.debug(f"Failed to parse KPI row at index {row_idx}: {e}")
            return None

    def _map_aggregation_type(self, excel_agg_type: str) -> str:
        """Map Excel aggregation type to technical aggregation type."""
        if not excel_agg_type:
            return DEFAULT_AGG_TYPE
        
        # Clean the input (lowercase, strip spaces)
        clean_agg_type = excel_agg_type.lower().strip()
        # typer.echo(f"DEBUG: Mapping aggregation type '{excel_agg_type}'")
        # typer.echo(f"DEBUG: aggregation type MAPPING '{AGG_TYPE_MAPPING}'")
        # Look up in mapping
        LOWER_AGG_TYPE_MAPPING = {k.lower().strip(): v for k, v in AGG_TYPE_MAPPING.items()}
        # typer.echo(f"DEBUG: LOWER_AGG_TYPE_MAPPING '{LOWER_AGG_TYPE_MAPPING}'")
        technical_agg_type = LOWER_AGG_TYPE_MAPPING.get(clean_agg_type, DEFAULT_AGG_TYPE)
        # typer.echo(f"DEBUG: Mapped to technical aggregation type '{technical_agg_type}'")
        if technical_agg_type != clean_agg_type:
            logger.debug(f"Mapped aggregation type: '{excel_agg_type}' -> '{technical_agg_type}'")
        
        # if clean_agg_type not in technical_agg_type:
        #     logger.warning(f"Unknown aggregation type in Excel: '{excel_agg_type}', using default: '{DEFAULT_AGG_TYPE}'")
        
        return technical_agg_type