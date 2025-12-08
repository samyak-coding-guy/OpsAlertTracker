import pandas as pd
import io
from typing import Optional
from openpyxl.styles import Alignment, Font

def create_excel_file(df: pd.DataFrame) -> bytes:
    """
    Convert a pandas DataFrame to an Excel file in memory with formatting.
    
    Args:
        df: Pandas DataFrame containing the processed alert data
    
    Returns:
        Excel file as bytes
    """

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Write DataFrame
        df.to_excel(writer, sheet_name='OpsGenie Alerts', index=False)
        
        worksheet = writer.sheets['OpsGenie Alerts']

        # Apply formatting to header
        header_font = Font(bold=True)
        for cell in next(worksheet.iter_rows(min_row=1, max_row=1)):
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center', vertical='center')

        # Auto-fit column width
        for column_cells in worksheet.columns:
            max_length = 0
            column_letter = column_cells[0].column_letter

            for cell in column_cells:
                try:
                    cell_value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(cell_value))
                except:
                    pass

            adjusted_width = min(max_length + 2, 60)  # capped at width 60
            worksheet.column_dimensions[column_letter].width = adjusted_width

        # Freeze top row
        worksheet.freeze_panes = 'A2'

        # Enable Excel filter
        worksheet.auto_filter.ref = worksheet.dimensions

    output.seek(0)
    return output.getvalue()