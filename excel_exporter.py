import pandas as pd
import io
from typing import Optional

def create_excel_file(df: pd.DataFrame) -> bytes:
    """
    Convert a pandas DataFrame to an Excel file in memory
    
    Args:
        df: Pandas DataFrame containing the alert data
        
    Returns:
        Excel file as bytes object
    """
    # Create a BytesIO object to store the Excel file
    output = io.BytesIO()
    
    # Create Excel writer
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Write the DataFrame to Excel
        df.to_excel(writer, sheet_name='OpsGenie Alerts', index=False)
        
        # Get the worksheet to apply formatting
        worksheet = writer.sheets['OpsGenie Alerts']
        
        # Auto-adjust columns' width
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            
            # Add some padding
            adjusted_width = max_length + 2
            
            # Limit max width to 50 characters to avoid overly wide columns
            if adjusted_width > 50:
                adjusted_width = 50
                
            worksheet.column_dimensions[column_letter].width = adjusted_width
        
        # Freeze the header row
        worksheet.freeze_panes = 'A2'
        
        # Create a filter for the header row
        worksheet.auto_filter.ref = worksheet.dimensions
    
    # Seek to the beginning of the file
    output.seek(0)
    
    # Return the Excel file as bytes
    return output.getvalue()
