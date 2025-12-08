import streamlit as st
import pandas as pd
import io
from datetime import datetime, time, timedelta, timezone
from opsgenie_client import OpsGenieClient
from data_processor import process_alerts
from excel_exporter import create_excel_file
from logger_config import setup_logger

logger = setup_logger(__name__)

st.set_page_config(
    page_title="JSM Alert Exporter",
    page_icon="📊",
    layout="wide"
)

st.title("Alert Data Exporter")
st.markdown("Fetch alert data and export it to Excel")

# Sidebar
with st.sidebar:
    st.header("API Configuration")
    api_key = st.text_input("API Key", type="password")

    st.header("Search Parameters")

    # Date range
    today = datetime.now().date()
    last_week = today - timedelta(days=7)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=last_week)
    with col2:
        end_date = st.date_input("End Date", value=today)

    if start_date > end_date:
        st.error("⚠️ End date must be after the start date.")
        st.stop()

    start_datetime = datetime.combine(start_date, time.min).replace(tzinfo=timezone.utc)
    end_datetime = datetime.combine(end_date, time.max).replace(tzinfo=timezone.utc)

    # Status filter
    status = st.selectbox(
        "Alert Status", 
        ["all", "open", "unacknowledged", "acknowledged", "closed"]
    )

    # Max results
    max_results_option = st.selectbox(
        "Maximum Alerts to Fetch",
        ["No Limit"] + list(range(10, 1010, 10))
    )

# MAIN PAGE
if not api_key:
    st.info("Please enter your API key to begin.")
else:

    if st.button("Fetch Alert Data", type="primary", use_container_width=True):
        try:
            with st.spinner("Connecting to API..."):
                client = OpsGenieClient(api_key)

                # Apply limits
                if isinstance(max_results_option, int):
                    max_results = max_results_option
                else:
                    max_results = None

                # Build status filter query
                if status == "open":
                    query = "status:open"
                elif status == "unacknowledged":
                    query = "status:open AND acknowledged:false"
                elif status == "acknowledged":
                    query = "status:open AND acknowledged:true"
                elif status == "closed":
                    query = "status:closed"
                else:
                    query = None

                # Fetch alerts
                alerts_data = client.get_alerts(
                    params={"query": query},
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    max_results=max_results
                )

            if not alerts_data:
                st.warning("No alerts found matching your criteria.")
                st.stop()

            progress_bar = st.progress(0)
            status_text = st.empty()

            alert_details = []

            # Fetch logs for each alert
            for i, alert in enumerate(alerts_data):
                progress_bar.progress((i + 1) / len(alerts_data))
                status_text.text(f"Fetching logs for alert {i+1} of {len(alerts_data)}...")

                logs = client.get_all_logs(alert["id"])

                # Append in new expected format
                alert_details.append({
                    "alert_id": alert["id"],
                    "logs": logs
                })

            progress_bar.empty()
            status_text.text("All logs fetched successfully!")

            # Process alert logs
            df = process_alerts(alert_details)

            if df.empty:
                st.warning("No data available after processing.")
            else:
                st.subheader("Alert Data Preview")
                st.dataframe(df)

                excel_file = create_excel_file(df)

                st.download_button(
                    label="Download Excel File",
                    data=excel_file,
                    file_name=f"opsgenie_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error(f"Error: {str(e)}")

# FOOTER
st.markdown("---")
st.caption("This tool connects to the API using a read-only API key.")