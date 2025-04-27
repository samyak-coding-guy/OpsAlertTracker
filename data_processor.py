import pandas as pd
import streamlit as st
from typing import List, Dict, Any, Optional
from dateutil import parser
from logger_config import setup_logger
import datetime
import pytz
import json

logger = setup_logger(__name__)

def process_alerts(alert_details: List[List[Dict[str, Any]]]) -> pd.DataFrame:

    """`````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````        
    Process the raw alert data from OpsGenie into a structured DataFrame
    
    Args:
        alert_details: List of alert detail dictionaries from OpsGenie
        
    Returns:
        Pandas DataFrame with processed alert information
    """

    # Log the number of alerts being processed
    logger.info(f"Processing {len(alert_details)} alerts.")

    # Log the alert details for debugging
    # Note: Be cautious with logging sensitive information
    logger.info("Alerts Data:")
    logger.info(json.dumps(alert_details, indent=2))

    # Check if alert_details is empty or None
    if not alert_details:
        return pd.DataFrame()
    
    # Initialize an empty list to store processed alert data
    processed_data = []
    
    # Iterate through each alert and extract relevant information
    for alert in alert_details:
        logger.info(f"Type of alertD: {type(alert_details)}")
        # Extract basic alert information
        alert_data = {
            'Alert ID': alert.get('id', ''),
            'Alert Title': alert.get('message', ''),
            'Status': alert.get('status', ''),
            'Priority': alert.get('priority', ''),
            'Created At': format_timestamp(alert.get('createdAt', '')),
            'Source':  alert.get('source', ''),
            'Acknowledged By': alert.get('report', {}).get('acknowledgedBy', '') if isinstance(alert.get('report'), dict) else '',
            'Acknowledged At': format_timestamp(alert.get('report', {}).get('ackTime', ''), alert.get('createdAt', '')) if isinstance(alert.get('report'), dict) else '',
            'Acknowledged via': '',
            'Current Owner': '',
            'Assign Ownership To': '',
            'Assigned At': '',
            'Closed By': alert.get('report', {}).get('closedBy', '') if isinstance(alert.get('report'), dict) else '',
            'Closed At': format_timestamp(alert.get('report', {}).get('closeTime', ''), alert.get('createdAt', '')) if isinstance(alert.get('report'), dict) else '',
            'Integration': alert.get('integration', {}).get('name', '') if alert.get('integration') else '',
        }
        
        # Owners might also be in the main alert data
        if not alert_data['Current Owner'] and alert.get('owner'):
            alert_data['Current Owner'] = alert.get('owner')
        
        processed_data.append(alert_data)
    
    # Create DataFrame and ensure columns are in a logical order
    df = pd.DataFrame(processed_data)
    
    # Reorganize columns for better readability
    column_order = [
        'Alert ID', 'Alert Title', 'Status', 'Priority',
        'Created At',
        'Acknowledged By', 'Acknowledged At', 'Acknowledged via', 'Current Owner',
        'Assign Ownership To', 'Assigned At',
        'Closed By', 'Closed At',
        'Source', 'Integration'
    ]
    
    # Only include columns that exist in the dataframe
    existing_columns = [col for col in column_order if col in df.columns]
    
    return df[existing_columns]

def format_timestamp(timestamp: Any, created_at: Optional[Any] = None) -> str:
    """
    Format a timestamp into a human-readable format in IST.
    The timestamp can be in ISO 8601 format or acknowledgment time in milliseconds.
    If the timestamp is in milliseconds (ack time), it is added to the created_at time to get the final IST time.
    
    Args:
        timestamp: ISO format timestamp string or time in milliseconds
        created_at: ISO format timestamp string representing the created time (optional)
        
    Returns:
        Formatted timestamp string in IST or empty string if invalid
    """
    if not timestamp:
        return ''
    
    try:
        # Check if the timestamp is in milliseconds
        if isinstance(timestamp, (int, float)):
            if created_at:
                # Parse created_at and add the milliseconds to it
                created_at_dt = parser.parse(created_at)
                created_at_utc = created_at_dt.replace(tzinfo=pytz.UTC) if created_at_dt.tzinfo is None else created_at_dt.astimezone(pytz.UTC)
                dt_utc = created_at_utc + datetime.timedelta(milliseconds=timestamp)
            else:
                # If created_at is not provided, treat timestamp as standalone milliseconds
                dt_utc = datetime.datetime.fromtimestamp(timestamp / 1000, tz=pytz.UTC)
        else:
            # Assume the timestamp is in ISO 8601 format
            dt = parser.parse(timestamp)
            dt_utc = dt.replace(tzinfo=pytz.UTC) if dt.tzinfo is None else dt.astimezone(pytz.UTC)
        
        # Convert UTC time to IST
        dt_ist = dt_utc.astimezone(pytz.timezone('Asia/Kolkata'))
        return dt_ist.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OverflowError):
        return ''
