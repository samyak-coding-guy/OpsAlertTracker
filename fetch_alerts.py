import sys
import datetime
from dateutil import parser
import argparse

from opsgenie_client import OpsGenieClient
from data_processor import process_alerts
from excel_exporter import create_excel_file

def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetch OpsGenie alerts and export to Excel')
    parser.add_argument('--api-key', required=True, help='OpsGenie API key')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look back (default: 7)')
    parser.add_argument('--status', choices=['all', 'open', 'closed', 'acked'], default='all', 
                        help='Alert status filter (default: all)')
    parser.add_argument('--limit', type=int, default=100, 
                        help='Maximum number of alerts to fetch (default: 100)')
    parser.add_argument('--output', default='opsgenie_alerts.xlsx', 
                        help='Output Excel file name (default: opsgenie_alerts.xlsx)')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Calculate date range
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=args.days)
    
    print(f"Connecting to OpsGenie API...")
    client = OpsGenieClient(args.api_key)
    
    search_params = {
        "createdAt": f"{start_date.isoformat()}/{end_date.isoformat()}",
        "limit": args.limit
    }
    
    if args.status != "all":
        search_params["status"] = args.status
    
    print(f"Fetching alerts from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    alerts_data = client.get_alerts(search_params)
    
    if not alerts_data:
        print("No alerts found matching your criteria.")
        return
    
    print(f"Found {len(alerts_data)} alerts, fetching details...")
    
    # Get alert details for all alerts
    alert_details = []
    for i, alert in enumerate(alerts_data):
        progress = ((i + 1) / len(alerts_data)) * 100
        print(f"Processing alert {i+1} of {len(alerts_data)} ({progress:.1f}%)...", end='\r')
        
        # Fetch detailed information for each alert
        detail = client.get_alert_details(alert["id"])
        if detail:
            alert_details.append(detail)
    
    print("\nProcessing complete!                   ")
    
    # Process the alert data
    df = process_alerts(alert_details)
    
    if df.empty:
        print("No data to display after processing.")
        return
    
    # Export to Excel
    print(f"Creating Excel file: {args.output}")
    excel_data = create_excel_file(df)
    
    # Write to file
    with open(args.output, 'wb') as f:
        f.write(excel_data)
    
    print(f"Excel file created successfully with {len(df)} alerts.")
    print(f"Key columns included: Alert ID, Acknowledged By, Acknowledged At, Assigned To, Assigned At")

if __name__ == "__main__":
    main()