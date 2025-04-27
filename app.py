import streamlit as st
import pandas as pd
import io
from datetime import datetime, time, timedelta, timezone
from opsgenie_client import OpsGenieClient
from data_processor import process_alerts
from excel_exporter import create_excel_file
from logger_config import setup_logger

logger = setup_logger(__name__)

# Example usage
logger.info("Application started.")

st.set_page_config(
    page_title="OpsGenie Alert Exporter",
    page_icon="📊",
    layout="wide"
)

st.title("OpsGenie Alert Data Exporter")
st.markdown("Fetch alert data from OpsGenie and export it to Excel")

# Sidebar for input parameters
with st.sidebar:
    st.header("API Configuration")
    api_key = st.text_input("OpsGenie API Key", type="password", help="Your OpsGenie read-only API key")
    
    st.header("Search Parameters")
    
    # Date range selection
    st.subheader("Date Range")
    today = datetime.now().date()
    last_week = today - timedelta(days=7)
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=last_week)
    with col2:
        end_date = st.date_input("End Date", value=today)

    # Validate date range
    if start_date > end_date:
       st.error("⚠️ End date must be after the start date.")
       st.stop()    
    
    # Convert to timezone-aware datetime objects
    start_datetime = datetime.combine(start_date, time.min).replace(tzinfo=timezone.utc)
    end_datetime = datetime.combine(end_date, time.max).replace(tzinfo=timezone.utc)

    # Additional filters
    st.subheader("Additional Filters")
    status_options = ["all", "open", "unacknowledged", "acknowledged", "closed"]
    status = st.selectbox("Alert Status", status_options, index=0)
    
    # Maximum alerts to fetch with "No Limit" option
    max_results_option = st.selectbox(
        "Maximum Alerts to Fetch",
        options=["No Limit"] + list(range(10, 1010, 10)),
        help="Select 'No Limit' to fetch all alerts within the date range"
    )

# Main content area
if not api_key:
    st.info("Please enter your OpsGenie API key in the sidebar to begin.")
else:
    fetch_button = st.button("Fetch Alert Data", type="primary", use_container_width=True)
    
    if fetch_button:
        try:
            with st.spinner("Connecting to OpsGenie API..."):
                client = OpsGenieClient(api_key)
                
                search_params = {
                    "createdAt": f"{start_datetime.isoformat()}/{end_datetime.isoformat()}",
                    "sort": "createdAt",
                    "order": "desc"
                }
                
                # Set limit if not "No Limit"
                if isinstance(max_results_option, int):
                    search_params["limit"] = max_results_option
                    max_results = max_results_option
                else:
                    max_results = None  # No limit
                
                # Construct the query based on selected status
                if status == "open":
                    query = "status:open"
                elif status == "unacknowledged":
                    query = "status:open AND acknowledged:false"
                elif status == "acknowledged":
                    query = "status:open AND acknowledged:true"
                elif status == "closed":
                    query = "status:closed"
                else:
                    query = None  # "all" selected, no filter

                
                alerts_data = client.get_alerts(
                    params={"query": query},
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    max_results=max_results if max_results != 0 else None
                )

            if not alerts_data:
                st.warning("No alerts found matching your criteria.")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                status_text.text("Processing alerts...")
                
                # Get alert details for all alerts
                alert_details = []
                for i, alert in enumerate(alerts_data):
                    progress = (i + 1) / len(alerts_data)
                    progress_bar.progress(progress)
                    status_text.text(f"Processing alert {i+1} of {len(alerts_data)}")
                    
                    # Fetch detailed information for each alert
                    detail = client.get_alert_details(alert["id"])
                    if detail:
                        alert_details.append(detail)
                
                status_text.text("Processing complete!")
                progress_bar.empty()

                # Flatten the list of alert details if necessary
                if alert_details and isinstance(alert_details[0], list):
                   alert_details = alert_details[0]
                
                # Process the alert data
                df = process_alerts(alert_details)
                print(f"Processed {len(df)} alerts.")
                if df.empty:
                    st.warning("No data to display after processing.")
                else:
                    # Display the data
                    st.subheader("Alert Data Preview")
                    st.dataframe(df)
                    
                    # Export to Excel
                    excel_file = create_excel_file(df)
                    
                    # Create a download button
                    st.download_button(
                        label="Download Excel File",
                        data=excel_file,
                        file_name=f"opsgenie_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        except Exception as e:
            st.error(f"Error: {str(e)}")

# Footer
st.markdown("---")
st.caption("This tool connects to the OpsGenie API to fetch alert data. Make sure you're using a read-only API key.")
