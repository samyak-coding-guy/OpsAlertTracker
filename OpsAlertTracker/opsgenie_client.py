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

    # ----------------------------------------------------------------------
    # FETCH ALERTS (unchanged)
    # ----------------------------------------------------------------------

    def get_alerts(
        self,
        params: Optional[Dict[str, Any]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        max_results: Optional[int] = None
    ) -> List[Dict[str, Any]]:

        if start_datetime and end_datetime and (end_datetime - start_datetime).days > 7:
            return self.get_alerts_in_chunks_parallel(
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                max_results=max_results,
                params=params
            )
        else:
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

        url = self.BASE_URL
        all_alerts: List[Dict[str, Any]] = []

        query_params = dict(params or {})
        default_limit = 100
        effective_limit = default_limit

        if max_results is not None and max_results > 0:
            effective_limit = min(max_results, default_limit)

        query_params["limit"] = effective_limit

        # createdAt filters
        query_filters = []
        if start_datetime:
            query_filters.append(f"createdAt>={int(start_datetime.timestamp())}")
        if end_datetime:
            query_filters.append(f"createdAt<={int(end_datetime.timestamp())}")

        if query_filters:
            created_query = " ".join(query_filters)
            existing_query = (query_params.get("query") or "").strip()
            query_params["query"] = f"{existing_query} {created_query}".strip()

        # Pagination loop
        while url:
            try:
                resp = requests.get(url, headers=self.headers, params=query_params)
                resp.raise_for_status()
                payload = resp.json()

                batch = payload.get("data", [])
                all_alerts.extend(batch)

                # stop if enough
                if max_results is not None and len(all_alerts) >= max_results:
                    all_alerts = all_alerts[:max_results]
                    break

                url = payload.get("paging", {}).get("next")
                query_params = None
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                try:
                    error = resp.json().get("message", str(e))
                except:
                    error = str(e)
                raise Exception(f"Failed to fetch alerts: {error}")

        return all_alerts

    def get_alerts_in_chunks_parallel(
        self,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        max_results: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:

        all_alerts: List[Dict[str, Any]] = []

        current_end = end_datetime
        week_chunks = []

        while current_end > start_datetime:
            current_start = max(current_end - datetime.timedelta(days=7), start_datetime)
            week_chunks.append((current_start, current_end))
            current_end = current_start

        logging.info(f"Fetching {len(week_chunks)} chunks in parallel...")

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    self.get_alerts_sequential,
                    start_datetime=s,
                    end_datetime=e,
                    params=params,
                    max_results=None
                )
                for s, e in week_chunks
            ]

            for future in as_completed(futures):
                try:
                    alerts = future.result()
                    all_alerts.extend(alerts)
                except Exception as e:
                    logging.error(f"Chunk fetch error: {e}")

        all_alerts.sort(key=lambda a: a.get("createdAt", 0), reverse=True)

        if max_results:
            all_alerts = all_alerts[:max_results]

        return all_alerts

    # ----------------------------------------------------------------------
    # FETCH ALL LOGS FOR AN ALERT (NEW)
    # ----------------------------------------------------------------------

    def get_all_logs(self, alert_id: str) -> List[Dict[str, Any]]:
        """Fetch ALL log entries for an alert (handles pagination)."""
        url = f"{self.BASE_URL}/{alert_id}/logs"
        logs = []

        while url:
            try:
                resp = requests.get(url, headers=self.headers)
                resp.raise_for_status()
                payload = resp.json()

                logs.extend(payload.get("data", []))

                # check pagination
                url = payload.get("paging", {}).get("next")
                time.sleep(0.3)

            except requests.exceptions.RequestException as e:
                logging.error(f"Failed fetching logs for {alert_id}: {e}")
                break

        return logs

    # ----------------------------------------------------------------------
    # (OLD) get_alert_details — NOW POINTS TO get_all_logs
    # ----------------------------------------------------------------------

    def get_alert_details(self, alert_id: str) -> Optional[List[Dict[str, Any]]]:
        """Backward compatible — returns all logs."""
        logs = self.get_all_logs(alert_id)
        return logs if logs else None

    # ----------------------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------------------

    def export_alerts_to_excel(self, alerts: List[Dict[str, Any]], filename: str):
        if not alerts:
            print("No alerts to export.")
            return

        try:
            df = pd.DataFrame(alerts)
            df.to_excel(filename, index=False)
            print(f"Exported to {filename}")
        except Exception as e:
            logging.error(f"Excel export error: {e}")