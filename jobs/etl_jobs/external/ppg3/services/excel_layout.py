from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from copy import copy

logger = logging.getLogger(__name__)


class ExcelLayoutService:
    """Handles Excel layout operations like date column expansion and styling."""

    def __init__(self):
        pass
    
    @staticmethod
    def expand_date_columns_kpis(
        worksheet, 
        sheet_definition: str, 
        ds: str, 
        min_year: int = 2021, 
        nblank_cols: int = 1
    ) -> Optional[Dict[str, Any]]:
        """
        Expand date columns in KPI sheets based on ds (YYYY-MM).
        
        Args:
            worksheet: openpyxl worksheet instance
            sheet_definition: "individual_kpis" or "collective_kpis" 
            ds: Consolidation date in YYYY-MM-DD format
            min_year: Minimum year to include in expansion
            nblank_cols: Number of blank columns to insert
            
        Returns:
            Date mappings dict with years and months, None if failed
        """
        try:
            if sheet_definition == "individual_kpis":
                return ExcelLayoutService._expand_date_columns_indiv(
                    worksheet, ds, min_year, nblank_cols
                )
            elif sheet_definition == "collective_kpis":
                return ExcelLayoutService._expand_date_columns_collective(
                    worksheet, ds, min_year, nblank_cols
                )
            else:
                logger.warning(f"Unknown sheet definition for date expansion: {sheet_definition}")
                return None
                
        except Exception as e:
            logger.warning(f"Failed to expand date columns for {sheet_definition}: {e}")
            return None
    
    @staticmethod 
    def _expand_date_columns_indiv(
        worksheet, 
        ds: str, 
        min_year: int = 2021, 
        nblank_cols: int = 1
    ) -> Dict[str, Any]:
        """
        Expand columns in individual_kpis sheet based on ds (YYYY-MM).
        Copies template columns fully and updates header/date cells.
        Includes blank column and subsequent template columns (e.g., months).
        """
        ds_year, ds_month, _ = map(int, ds.split("-"))

        # Layout info - using hardcoded values since SHEET_LAYOUT not available
        title_row_offset = 0
        title_height = 3
        start_row = title_row_offset + title_height + 1
        header_row = worksheet[start_row]

        # Locate template "YYYY" column
        template_col_idx = None
        for idx, cell in enumerate(header_row[:10], start=1):
            if cell.value == "YYYY":
                template_col_idx = idx
                break
        if template_col_idx is None:
            raise ValueError("No 'YYYY' template column found in first 10 columns of the KPI sheet.")
        
        month_shifts = [-2, -1, -13]
        insert_idx = template_col_idx + nblank_cols + len(month_shifts) + 1
        
        # Build mappings
        years_mapping, months_mapping = [], []
        mapping_offset = nblank_cols + len(month_shifts) + 1
        
        # 1) Insert past years until ds_year-1
        for year in range(min_year, ds_year):
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx - 1]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
            label = f"{year}"
            worksheet.cell(row=start_row, column=insert_idx, value=label)
            years_mapping.append({int(insert_idx - mapping_offset): label})
            insert_idx += 1
            
        # 2) Insert blank columns
        for i in range(nblank_cols):
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx + i]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
            insert_idx += 1
            
        # 3) Insert month columns from template
        for month in month_shifts:
            target_date = datetime(ds_year, ds_month, 1) + relativedelta(months=month)
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx - 1]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
            label = target_date.strftime("%m/%Y")
            worksheet.cell(row=start_row, column=insert_idx, value=label)
            months_mapping.append({int(insert_idx - mapping_offset): label})
            insert_idx += 1
            
        # 4) Remove template columns
        worksheet.delete_cols(template_col_idx, nblank_cols + len(month_shifts) + 1)
        
        return {"date_mappings": {"years": years_mapping, "months": months_mapping}}
    
    @staticmethod
    def _expand_date_columns_collective(
        worksheet, 
        ds: str, 
        min_year: int = 2021, 
        nblank_cols: int = 1
    ) -> Dict[str, Any]:
        """
        Expand columns in collective_kpis sheet based on ds (YYYY-MM).
        Copies template columns fully and only updates header/date cells.
        """
        ds_year, ds_month, _ = map(int, ds.split("-"))

        # Layout info - using hardcoded values since SHEET_LAYOUT not available  
        title_row_offset = 0
        title_height = 3
        start_row = title_row_offset + title_height + 1
        header_row = worksheet[start_row]

        # Locate template "YYYY" column
        template_col_idx = None
        for idx, cell in enumerate(header_row[:10], start=1):
            if cell.value == "YYYY":
                template_col_idx = idx
                break
        if template_col_idx is None:
            raise ValueError("No 'YYYY' template column found in first 10 columns of the KPI sheet.")
        
        # 1) Insert past years until ds_year-1
        ds_year_scholar = ds_year if ds_month >= 9 else ds_year - 1
        month_shifts = [-2, -1, -13]
        insert_idx = template_col_idx + nblank_cols + len(month_shifts) + 1

        # Build mappings
        years_mapping, months_mapping = [], []
        mapping_offset = nblank_cols + len(month_shifts) + 1
        
        for year in range(min_year, ds_year_scholar):
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx - 1]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
                    
            label = f"{year}-{year+1}"
            worksheet.cell(row=start_row, column=insert_idx, value=label)
            years_mapping.append({int(insert_idx - mapping_offset): label})
            insert_idx += 1

        # 2) Insert blank columns
        for i in range(nblank_cols):
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx + i]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
            insert_idx += 1
            
        # 3) Insert month columns from template
        for month in month_shifts:
            target_date = datetime(ds_year, ds_month, 1) + relativedelta(months=month)
            worksheet.insert_cols(insert_idx)
            for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row):
                src_cell = row[template_col_idx - 1]
                dest_cell = row[insert_idx - 1]
                if src_cell.has_style:
                    dest_cell._style = copy(src_cell._style)
                    
            label = target_date.strftime("%m/%Y")
            worksheet.cell(row=start_row, column=insert_idx, value=label)
            months_mapping.append({int(insert_idx - mapping_offset): label})
            insert_idx += 1
            
        # 4) Remove template columns
        worksheet.delete_cols(template_col_idx, nblank_cols + len(month_shifts) + 1)
        
        return {"date_mappings": {"years": years_mapping, "months": months_mapping}}
    
    @staticmethod
    def set_sheet_title(
        worksheet, 
        title_base: str, 
        layout_type: str,
        context: Dict[str, Any],
        filters: Dict[str, Any]
    ):
        """
        Insert title in the worksheet according to layout rules.
        
        Args:
            worksheet: openpyxl worksheet
            title_base: Base title text
            layout_type: "top", "kpis", or "other"
            context: Sheet context (region, academy, department values)
            filters: Sheet filters (scale, scope, etc.)
        """
        try:
            # Layout configuration - hardcoded since SHEET_LAYOUT not available
            if layout_type in ["kpis", "top"]:
                row_offset, col_offset = 0, 3
            else:
                row_offset, col_offset = 0, 0
                
            # Build title
            title = title_base
            if layout_type in ["kpis", "top"]:
                scale = filters.get("scale", "")
                node_tag = context.get(scale)
                if node_tag:
                    title += "\n" + node_tag.capitalize()

            # Insert into worksheet
            worksheet.cell(row=row_offset + 1, column=col_offset + 1, value=title)
            
        except Exception as e:
            logger.warning(f"Failed to set sheet title '{title_base}': {e}")