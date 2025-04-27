import requests
import logging
import time
import datetime
import pandas as pd
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed


class OpsGenieClient:
    BASE_URL = "https://api.opsgenie.com/v2/alerts"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Authorization": f"GenieKey {api_key}",
            "Content-Type": "application/json",
        }

    def get_alerts(
        self,
        params: Optional[Dict[str, Any]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch alerts via pagination (honoring `limit` if given), optimized by chunking large date ranges.
        """
        # If a large date range is provided, split it into weekly chunks for parallel processing
        if start_datetime and end_datetime and (end_datetime - start_datetime).days > 7:
            return self.get_alerts_in_chunks_parallel(
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                max_results=max_results
            )
        else:
            # If the date range is small enough, fetch it normally
            return self.get_alerts_sequential(
                params=params,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                max_results=max_results
            )

    def get_alerts_sequential(
        self,
        params: Optional[Dict[str, Any]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch alerts sequentially without chunking.
        """
        url = self.BASE_URL
        all_alerts: List[Dict[str, Any]] = []

        query_params = dict(params or {})
        default_limit = 100
        effective_limit = default_limit

        if max_results is not None and max_results > 0:
            effective_limit = min(max_results, default_limit)

        query_params["limit"] = effective_limit

        # Apply createdAt filters
        query_filters: List[str] = []
        if start_datetime:
            query_filters.append(f"createdAt>={int(start_datetime.timestamp())}")
        if end_datetime:
            query_filters.append(f"createdAt<={int(end_datetime.timestamp())}")

        if query_filters:
            created_query = " ".join(query_filters)
            existing_query = (query_params.get("query") or "").strip()
            query_params["query"] = f"{existing_query} {created_query}".strip()

        # Start fetching
        while url:
            try:
                resp = requests.get(url, headers=self.headers, params=query_params)
                resp.raise_for_status()
                payload = resp.json()

                batch = payload.get("data", [])
                all_alerts.extend(batch)

                # Check if we have enough results
                if max_results is not None and len(all_alerts) >= max_results:
                    all_alerts = all_alerts[:max_results]
                    break

                # next page
                url = payload.get("paging", {}).get("next")
                query_params = None  # Only for first request
                time.sleep(0.5)
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                try:
                    error_data = resp.json()
                    if "message" in error_data:
                        error_msg = error_data["message"]
                except Exception:
                    pass
                raise Exception(f"Failed to fetch alerts: {error_msg}")

        return all_alerts

    def get_alerts_in_chunks_parallel(
        self,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch alerts by breaking the date range into weekly chunks and fetching them in parallel.
        """
        all_alerts: List[Dict[str, Any]] = []

        # Break the date range into weekly chunks
        current_start_date = start_datetime
        week_chunks = []

        # Generate chunks for parallel fetching
        while current_start_date < end_datetime:
            current_end_date = min(current_start_date + datetime.timedelta(days=7), end_datetime)
            week_chunks.append((current_start_date, current_end_date))
            current_start_date = current_end_date

        # Fetch each chunk in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            futures = []
            for start_date, end_date in week_chunks:
                futures.append(
                    executor.submit(self.get_alerts_sequential, start_datetime=start_date, end_datetime=end_date, max_results=max_results)
                )

            # Collect the results as they complete
            for future in as_completed(futures):
                try:
                    alerts = future.result()
                    all_alerts.extend(alerts)
                except Exception as e:
                    logging.error(f"Error fetching alerts: {e}")

        return all_alerts

    def get_alert_details(self, alert_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed information for a specific alert.
        """
        url = f"{self.BASE_URL}/{alert_id}"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            alert_data = response.json()["data"]
            return alert_data
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch details for alert {alert_id}: {str(e)}")
            return None

    def export_alerts_to_excel(self, alerts: List[Dict[str, Any]], filename: str):
        """
        Export alerts to an Excel file.
        """
        if not alerts:
            print("No alerts to export.")
            return

        try:
            df = pd.DataFrame(alerts)
            df.to_excel(filename, index=False)
            print(f"Alerts exported to {filename}")
        except Exception as e:
            logging.error(f"Error exporting alerts to Excel: {e}")
