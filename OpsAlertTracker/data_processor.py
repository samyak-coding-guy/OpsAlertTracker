import pandas as pd
from dateutil import parser
import pytz
import datetime
from typing import List, Dict, Any
from logger_config import setup_logger

logger = setup_logger(__name__)

def process_alerts(alert_details: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Process alert logs and extract:
    - Alert creation time
    - First acknowledgment
    - Assigned ownership
    - Escalation
    - Tag added
    - Closed event
    """

    if not alert_details:
        return pd.DataFrame()

    processed_rows = []

    for alert_entry in alert_details:
        alert_id = alert_entry.get("alert_id")
        logs = alert_entry.get("logs", [])

        if not logs:
            continue

        # Sort logs by time ASC (oldest first)
        logs.sort(key=lambda x: x.get("createdAt"))

        # Data containers
        ack_by = ack_at = ""
        assigned_to = assigned_at = ""
        escalated_to = escalated_at = ""
        closed_by = closed_at = ""
        tag_added = tag_added_at = ""

        # Alert creation time = First log timestamp
        created_at = format_timestamp(logs[0].get("createdAt"))

        # Process logs
        for log in logs:

            message = log.get("log", "")
            owner = log.get("owner", "")
            time_ist = format_timestamp(log.get("createdAt"))

            # FIRST ACKNOWLEDGEMENT
            if ("Alert acknowledged" in message) and not ack_by:
                ack_by = owner
                ack_at = time_ist

            # OWNERSHIP ASSIGNED
            if "Alert ownership assigned to" in message:
                assigned_to = extract_bracket_value(message)
                assigned_at = time_ist

            # ESCALATED
            if "Alert escalated to next level" in message:
                escalated_to = extract_bracket_value(message)
                escalated_at = time_ist

            # TAG ADDED
            if "added as a tag" in message.lower() and not tag_added:
                tag_added = extract_bracket_value(message)
                tag_added_at = time_ist

            # CLOSED
            if "Alert closed" in message:
                closed_by = owner
                closed_at = time_ist

        processed_rows.append({
            "Alert ID": alert_id,
            "Created At": created_at,
            "1st Alert Acknowledged By": ack_by,
            "1st Alert Acknowledged At": ack_at,
            "Assigned Ownership To": assigned_to,
            "Assigned At": assigned_at,
            "Escalated To": escalated_to,
            "Escalated At": escalated_at,
            "Tag Added": tag_added,
            "Tag Added At": tag_added_at,
            "Closed By": closed_by,
            "Closed At": closed_at
        })

    df = pd.DataFrame(processed_rows)

    column_order = [
        "Alert ID",
        "Created At",
        "1st Alert Acknowledged By",
        "1st Alert Acknowledged At",
        "Assigned Ownership To",
        "Assigned At",
        "Escalated To",
        "Escalated At",
        "Tag Added",
        "Tag Added At",
        "Closed By",
        "Closed At"
    ]

    return df[column_order]


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def extract_bracket_value(text: str) -> str:
    """
    Extract text inside the FIRST [...] bracket.
    """
    if "[" in text and "]" in text:
        return text.split("[", 1)[1].split("]", 1)[0]
    return ""


def format_timestamp(timestamp: Any) -> str:
    """
    Convert ISO timestamp into IST formatted string.
    """
    if not timestamp:
        return ""

    try:
        dt = parser.parse(timestamp)
        dt_utc = dt.astimezone(pytz.UTC)
        dt_ist = dt_utc.astimezone(pytz.timezone("Asia/Kolkata"))
        return dt_ist.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return ""