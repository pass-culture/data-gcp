from typing import Dict, List, Any, Optional
import logging
import pandas as pd
import typer
import math

logger = logging.getLogger(__name__)


class ExcelWriterService:
    """Handles writing data to Excel worksheets."""
    
    @staticmethod
    def write_kpi_data_to_sheet(
        worksheet,
        kpi_data: Dict[str, Dict[str, float]],
        date_mappings: Dict[str, Any],
        row_idx: int
    ) -> bool:
        """Write KPI data to Excel sheet using date mappings."""
        try:
            if not kpi_data:
                logger.warning(f"No KPI data to write for row {row_idx}")
                return False
                
            success_count = 0
            
            # Write yearly data
            yearly_data = kpi_data.get("yearly", {})
            years_mapping = date_mappings.get("years", [])
            
            
            for mapping in years_mapping:
                col_idx = list(mapping.keys())[0] - 1
                year_label = list(mapping.values())[0]
                
                value = ExcelWriterService._get_yearly_value(yearly_data, year_label)
                
                if value is not None:
                    excel_row = row_idx + 1
                    excel_col = col_idx + 1
                    
                    success = ExcelWriterService._write_cell_value(
                        worksheet, excel_row, excel_col, value
                    )
                    if success:
                        success_count += 1
                else:
                    pass
                    # print(f"DEBUG: No value found for year_label '{year_label}'")
            
            # Write monthly data (simplified for now)
            # monthly_data = kpi_data.get("monthly", {})
            # months_mapping = date_mappings.get("months", [])
            
            # # Convert monthly_data timestamps to string format for matching
            # monthly_data_formatted = {}
            # for timestamp_key, value in monthly_data.items():
            #     if hasattr(timestamp_key, 'strftime'):
            #         formatted_key = timestamp_key.strftime("%m/%Y")
            #         monthly_data_formatted[formatted_key] = value
            monthly_data = kpi_data.get("monthly", {})  # Now already a dict with string keys
            months_mapping = date_mappings.get("months", [])

            # No timestamp conversion needed - already in MM/YYYY format
            for mapping in months_mapping:
                col_idx = list(mapping.keys())[0] - 1
                month_label = list(mapping.values())[0]
                
                if month_label in monthly_data:  # Direct string comparison
                    success = ExcelWriterService._write_cell_value(
                        worksheet, row_idx + 1, col_idx + 1, monthly_data[month_label]
                    )
                    if success:
                        success_count += 1
            # print(f"DEBUG: monthly_data_formatted dict: {monthly_data_formatted}")

            # for mapping in months_mapping:
            #     col_idx = list(mapping.keys())[0] - 1
            #     month_label = list(mapping.values())[0]
                

            #     if month_label in monthly_data_formatted:
            #         # print(f"DEBUG: Found match using 'in' operator")
            #         success = ExcelWriterService._write_cell_value(
            #             worksheet, row_idx + 1, col_idx + 1, monthly_data_formatted[month_label]
            #         )
            #         if success:
            #             success_count += 1
            
            logger.debug(f"Wrote {success_count} values to row {row_idx}")
            return success_count > 0
            
        except Exception as e:
            logger.warning(f"Failed to write KPI data to row {row_idx}: {e}")
            print(f"DEBUG EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @staticmethod
    def _get_yearly_value(yearly_data: Dict, year_label: str) -> Optional[float]:
        """
        Extract yearly value handling both simple years (2021) and scholar years (2021-2022).
        
        Args:
            yearly_data: Dict mapping years to values
            year_label: Year label from date mapping
            
        Returns:
            Corresponding value or None if not found
        """
        try:
            # Handle simple year format (2021)
            if year_label.isdigit():
                year_int = int(year_label)
                return yearly_data.get(year_int)
            
            # Handle scholar year format (2021-2022) 
            if "-" in year_label:
                year_parts = year_label.split("-")
                if len(year_parts) == 2:
                    # Try both the scholar year label and the start year
                    if year_label in yearly_data:
                        return yearly_data[year_label]
                    # Fallback to start year as integer
                    try:
                        start_year = int(year_parts[0])
                        return yearly_data.get(start_year)
                    except ValueError:
                        pass
            
            # Direct lookup as string
            return yearly_data.get(year_label)
            
        except Exception as e:
            logger.debug(f"Failed to get yearly value for '{year_label}': {e}")
            return None
    
    @staticmethod
    def _write_cell_value(worksheet, row: int, col: int, value: Any) -> bool:
        """Write value to Excel cell with error handling."""
        try:
            # Validate and clean value
            if value is None or pd.isna(value):
                return False
                
            # Handle different value types
            if isinstance(value, (int, float)):
                if pd.isna(value) or math.isinf(value):
                    return False
                worksheet.cell(row=row, column=col, value=float(value))
            else:
                worksheet.cell(row=row, column=col, value=str(value))
            
            return True
            
        except Exception as e:
            logger.debug(f"Failed to write value {value} to cell ({row}, {col}): {e}")
            return False
    
    @staticmethod
    def write_top_data_to_sheet(
        worksheet,
        top_data: List[Dict[str, Any]],
        start_row: int = 1,
        max_rows: int = 50
    ) -> bool:
        """
        Write top rankings data to Excel sheet.
        
        Args:
            worksheet: openpyxl worksheet instance
            top_data: List of dicts with ranking data
            start_row: Starting row (1-based)
            max_rows: Maximum number of rows to write
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not top_data:
                logger.warning("No top data to write")
                return False
            
            # Limit data to max_rows
            limited_data = top_data[:max_rows]
            
            success_count = 0
            for i, row_data in enumerate(limited_data):
                row_idx = start_row + i
                col_idx = 1
                
                # Write each field in the row_data dict
                for field_name, value in row_data.items():
                    success = ExcelWriterService._write_cell_value(
                        worksheet, row_idx, col_idx, value
                    )
                    if success:
                        success_count += 1
                    col_idx += 1
            
            logger.debug(f"Wrote {success_count} values for top data")
            return success_count > 0
            
        except Exception as e:
            logger.warning(f"Failed to write top data: {e}")
            return False