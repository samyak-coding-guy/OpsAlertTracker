import pandas as pd
from typing import List, Dict, Any, Optional
from dateutil import parser
import datetime

def process_alerts(alert_details: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process the raw alert data from OpsGenie into a structured DataFrame
    
    Args:
        alert_details: List of alert detail dictionaries from OpsGenie
        
    Returns:
        Pandas DataFrame with processed alert information
    """
    if not alert_details:
        return pd.DataFrame()
    
    processed_data = []
    
    for alert in alert_details:
        # Extract basic alert information
        alert_data = {
            'Alert ID': alert.get('id', ''),
            'Alert Alias': alert.get('alias', ''),
            'Message': alert.get('message', ''),
            'Description': alert.get('description', ''),
            'Status': alert.get('status', ''),
            'Priority': alert.get('priority', ''),
            'Created At': format_timestamp(alert.get('createdAt', '')),
            'Updated At': format_timestamp(alert.get('updatedAt', '')),
            'Source': alert.get('source', ''),
            'Acknowledged By': '',
            'Acknowledged At': '',
            'Assigned To': '',
            'Assigned At': '',
            'Closed By': '',
            'Closed At': '',
            'Integration': alert.get('integration', {}).get('name', '') if alert.get('integration') else '',
            'Tags': ', '.join(alert.get('tags', [])),
        }
        
        # Process acknowledgment and assignment information from logs
        if 'logs' in alert:
            for log in alert['logs']:
                log_type = log.get('type', '')
                owner = log.get('owner', '')
                created_at = format_timestamp(log.get('createdAt', ''))
                
                # Acknowledgment information
                if log_type == 'AcknowledgeAlert' and not alert_data['Acknowledged By']:
                    alert_data['Acknowledged By'] = owner
                    alert_data['Acknowledged At'] = created_at
                
                # Assignment information
                elif log_type == 'AssignOwnership' and not alert_data['Assigned To']:
                    alert_data['Assigned To'] = owner
                    alert_data['Assigned At'] = created_at
                
                # Closed information
                elif log_type == 'CloseAlert' and not alert_data['Closed By']:
                    alert_data['Closed By'] = owner
                    alert_data['Closed At'] = created_at
        
        # Owners might also be in the main alert data
        if not alert_data['Assigned To'] and alert.get('owner'):
            alert_data['Assigned To'] = alert.get('owner')
        
        processed_data.append(alert_data)
    
    # Create DataFrame and ensure columns are in a logical order
    df = pd.DataFrame(processed_data)
    
    # Reorganize columns for better readability
    column_order = [
        'Alert ID', 'Alert Alias', 'Message', 'Status', 'Priority',
        'Created At', 'Updated At', 
        'Acknowledged By', 'Acknowledged At',
        'Assigned To', 'Assigned At',
        'Closed By', 'Closed At',
        'Source', 'Integration', 'Tags', 'Description'
    ]
    
    # Only include columns that exist in the dataframe
    existing_columns = [col for col in column_order if col in df.columns]
    
    return df[existing_columns]

def format_timestamp(timestamp: str) -> str:
    """
    Format an ISO timestamp into a human-readable format
    
    Args:
        timestamp: ISO format timestamp string
        
    Returns:
        Formatted timestamp string or empty string if invalid
    """
    if not timestamp:
        return ''
    
    try:
        dt = parser.parse(timestamp)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return timestamp  # Return original if parsing fails
