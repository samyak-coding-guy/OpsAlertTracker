import requests
import logging
from typing import Dict, List, Optional, Any
import time

class OpsGenieClient:
    """
    Client for interacting with the OpsGenie API
    """
    BASE_URL = "https://api.opsgenie.com/v2"
    
    def __init__(self, api_key: str):
        """
        Initialize the OpsGenie client with the provided API key
        
        Args:
            api_key: OpsGenie API key (read-only is sufficient)
        """
        self.api_key = api_key
        self.headers = {
            "Authorization": f"GenieKey {api_key}",
            "Content-Type": "application/json"
        }
        
    def get_alerts(self, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Fetch alerts from OpsGenie API with pagination
        
        Args:
            params: Dictionary of search parameters
            
        Returns:
            List of alert dictionaries
        """
        url = f"{self.BASE_URL}/alerts"
        all_alerts = []
        
        # Default params
        if params is None:
            params = {}
        
        # Set some defaults if not provided
        if "limit" not in params:
            params["limit"] = 100
            
        # Handle pagination
        offset = 0
        more_data = True
        
        while more_data:
            current_params = params.copy()
            current_params["offset"] = offset
            
            try:
                response = requests.get(url, headers=self.headers, params=current_params)
                response.raise_for_status()
                
                data = response.json()
                
                if "data" in data and data["data"]:
                    alerts = data["data"]
                    all_alerts.extend(alerts)
                    
                    # Check if we need to fetch more
                    if len(alerts) < params["limit"]:
                        more_data = False
                    else:
                        offset += len(alerts)
                        # Add a small delay to avoid rate limiting
                        time.sleep(0.5)
                else:
                    more_data = False
                    
            except requests.exceptions.RequestException as e:
                error_msg = str(e)
                try:
                    error_data = response.json()
                    if "message" in error_data:
                        error_msg = error_data["message"]
                except:
                    pass
                
                raise Exception(f"Failed to fetch alerts: {error_msg}")
        
        return all_alerts
    
    def get_alert_details(self, alert_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific alert including logs
        
        Args:
            alert_id: The ID of the alert to fetch
            
        Returns:
            Dictionary containing alert details or None if not found
        """
        url = f"{self.BASE_URL}/alerts/{alert_id}"
        
        try:
            # Get alert details
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            alert_data = response.json()["data"]
            
            # Get alert logs (for acknowledgment and assignment details)
            logs_url = f"{url}/logs"
            logs_response = requests.get(logs_url, headers=self.headers)
            logs_response.raise_for_status()
            logs_data = logs_response.json()["data"]
            
            # Add logs to the alert data
            alert_data["logs"] = logs_data
            
            return alert_data
            
        except requests.exceptions.RequestException as e:
            # Log the error but don't fail the entire process for one alert
            logging.error(f"Failed to fetch details for alert {alert_id}: {str(e)}")
            return None
