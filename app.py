import streamlit as st
import datetime
import pandas as pd
import io
from dateutil import parser

from opsgenie_client import OpsGenieClient
from data_processor import process_alerts
from excel_exporter import create_excel_file

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
    today = datetime.datetime.now().date()
    last_week = today - datetime.timedelta(days=7)
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=last_week)
    with col2:
        end_date = st.date_input("End Date", value=today)
    
    # Convert to datetime objects
    start_datetime = datetime.datetime.combine(start_date, datetime.time.min)
    end_datetime = datetime.datetime.combine(end_date, datetime.time.max)
    
    # Additional filters
    st.subheader("Additional Filters")
    status_options = ["all", "open", "closed", "acked"]
    status = st.selectbox("Alert Status", status_options, index=0)
    
    max_results = st.number_input("Maximum Alerts to Fetch", 
                                  min_value=10, 
                                  max_value=1000, 
                                  value=100, 
                                  step=10,
                                  help="Higher values may take longer to process")

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
                    "limit": max_results
                }
                
                if status != "all":
                    search_params["status"] = status
                
                alerts_data = client.get_alerts(search_params)
            
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
                
                # Process the alert data
                df = process_alerts(alert_details)
                
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
                        file_name=f"opsgenie_alerts_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        except Exception as e:
            st.error(f"Error: {str(e)}")

# Footer
st.markdown("---")
st.caption("This tool connects to the OpsGenie API to fetch alert data. Make sure you're using a read-only API key.")
