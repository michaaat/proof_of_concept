import requests
import datetime
import pytz
import time
import json
import re 
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field # --- NEW: Import dataclasses ---

# --- ServiceTitan Configuration ---
TIME_BEGIN = datetime.now()
CLIENT_ID = "cxxx"
CLIENT_SECRET = "cxxx"
TENANT_ID = 5000000000
APP_KEY = "axxx"

ST_AUTH_URL = 'https://auth.servicetitan.io/connect/token'
ST_API_URL = 'https://api.servicetitan.io/'

# --- Databox Global Configuration ---
DATABOX_API_KEY = "pak_xxx"
DATABOX_PAYLOAD_LIMIT = 100 

# --- Script Configuration ---
USE_CUSTOM_RANGE = True # Set to False to use date.today()
CUSTOM_START_DATE = date(2025, 11, 6)
CUSTOM_END_DATE = date(2025, 12, 3)

VERIFICATION_WAIT_TIME = 10  # Seconds
API_MAX_RETRIES = 5
API_INITIAL_BACKOFF = 1  # Seconds


# --- NEW: Report Configuration Dataclass ---
@dataclass
class ReportConfig:
    """A container for all report-specific configuration."""
    name: str
    report_id: int
    report_category: str
    databox_dataset_id: str
    body_template: Dict = field(default_factory=dict)
    
    # These properties automatically build the correct URLs
    @property
    def st_endpoint(self) -> str:
        """Builds the ServiceTitan endpoint URL."""
        return f"reporting/v2/tenant/{TENANT_ID}/report-category/{self.report_category}/reports/{self.report_id}/data"
    
    @property
    def databox_push_url(self) -> str:
        """Builds the Databox push URL."""
        return f"https://api.databox.com/v1/datasets/{self.databox_dataset_id}/data"
    
    @property
    def databox_verify_url(self) -> str:
        """Builds the Databox verification URL template."""
        return f"https://api.databox.com/v1/datasets/{self.databox_dataset_id}/ingestions/{{ingestion_id}}"

# --- NEW: Central Report Configuration List ---
# To add/remove/edit reports, YOU ONLY NEED TO EDIT THIS LIST.
ALL_REPORTS = [
    ReportConfig(
        name="Daily Leads Report",
        report_id=137162407,
        report_category="operations",
        databox_dataset_id="33cxx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 2},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ],
            "filters": [
                {"field": "JobType", "operator": "contains", "value": "Lead"},
                {"field": "CancelReason", "operator": "notin", "values": ["Training"]}
            ]
        }
    ),
    ReportConfig(
        name="Installed Jobs Report",
        report_id=145504741,
        report_category="operations",
        databox_dataset_id="0e0xx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 2},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ]
        }
    ),
    ReportConfig(
        name="Sold Estimate Status DFW/Waco",
        report_id=324291432,
        report_category="other",
        databox_dataset_id="80f6xx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 1},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ]
        }
    ),
    ReportConfig(
        name="Job on Hold Report",
        report_id=166439432,
        report_category="operations",
        databox_dataset_id="2fa1xxx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 3},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ]
        }
    ),
    ReportConfig(
        name="Closed Job Report",
        report_id=145504741,
        report_category="operations",
        databox_dataset_id="9bxx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 1},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ]
        }
    ),
    ReportConfig(
        name="Backlog Dollars Report",
        report_id=321988710,
        report_category="operations",
        databox_dataset_id="cbxxx",
        body_template={
            "parameters": [
                {"name": "DateType", "value": 2},
                {"name": "From", "value": "YYYY-MM-DD"},
                {"name": "To", "value": "YYYY-MM-DD"}
            ]
        }
    ),
]
# --- END OF CENTRAL CONFIGURATION ---


class ServiceTitan:
    """ServiceTitan class for API integration, using synchronous requests."""

    def __init__(self):
        self.accessToken = None
        self.headers = {}
        self.auth_data = {'grant_type': 'client_credentials', 'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

    def get_access_token(self) -> bool:
        """Fetches the OAuth 2.0 access token."""
        print("--- Authenticating with ServiceTitan...")
        try:
            response = requests.post(ST_AUTH_URL, data=self.auth_data, timeout=30)
            response.raise_for_status()
            token_data = response.json()
            self.accessToken = token_data.get('access_token')
            if self.accessToken:
                print("--- Successfully obtained access token. ---")
                self.headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {self.accessToken}',
                    'ST-App-Key': APP_KEY,
                    'Content-Type': 'application/json'
                }
                return True
            print("Authentication failed: Access token not found in response.")
            return False
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False

    def make_st_api_request(self, method: str, url: str, json_payload: Dict = None, is_retry: bool = False, max_retries=API_MAX_RETRIES) -> Dict:
        """
        Makes a synchronous API request to ServiceTitan, handling auth, retries, and rate limits.
        """
        retries = 0
        backoff_time = API_INITIAL_BACKOFF
        
        while retries < max_retries:
            try:
                if method.upper() == "POST":
                    response = requests.post(url, headers=self.headers, json=json_payload, timeout=60)
                else:
                    response = requests.get(url, headers=self.headers, timeout=60)

                # --- Handle Token Refresh (401) ---
                if response.status_code == 401 and not is_retry:
                    print("Token expired (401). Attempting to refresh...")
                    if self.get_access_token():
                        print("Token refreshed. Retrying original request...")
                        return self.make_st_api_request(method, url, json_payload, is_retry=True)
                    else:
                        raise Exception("Authentication refresh failed.")
                
                if response.status_code == 429:
                    print("Rate limit hit (429). Waiting 65 seconds...")
                    time.sleep(65)
                    continue 

                if response.status_code >= 500:
                    print(f"Server error ({response.status_code}). Retrying in {backoff_time}s...")
                    time.sleep(backoff_time)
                    backoff_time = min(backoff_time * 2, 60)
                    retries += 1
                    continue

                if 400 <= response.status_code < 500:
                    print(f"Client error: {response.status_code}. Not retrying.")
                    print(f"URL: {response.url}")
                    print(f"Response: {response.text}")
                    response.raise_for_status()

                if response.status_code == 200:
                    if not response.text:
                        print(f"Warning: Received empty 200 response for {url}")
                        return {}
                    return response.json()
                
                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                print(f"A network error occurred: {e}. Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
                retries += 1
        
        raise Exception(f"Max retries ({max_retries}) exceeded for request to {url}.")

    def fetch_report_data_for_range(self, report_endpoint: str, base_report_body: Dict, dates_to_process: List[date], report_name: str) -> Tuple[List[List], List[Dict]]:
        """
        Generic function to loop through dates, execute a report, and return all data.
        """
        all_data_rows = []
        report_fields = []
        
        for i, single_date in enumerate(dates_to_process):
            date_str = single_date.strftime('%Y-%m-%d')
            print(f"\n--- Fetching '{report_name}' for {date_str} ({i+1}/{len(dates_to_process)}) ---")

            report_body = json.loads(json.dumps(base_report_body)) # Deep copy
            
            # Update date parameters for the current loop
            for param in report_body.get("parameters", []):
                if param["name"] == "From":
                    param["value"] = date_str
                if param["name"] == "To":
                    param["value"] = date_str
            
            print(f"Executing with body: {json.dumps(report_body)}")

            try:
                result = self.make_st_api_request("POST", ST_API_URL + report_endpoint, json_payload=report_body)
                
                if result and not report_fields:
                    report_fields = result.get('fields', [])
                    if not report_fields:
                        print(f"Error: {report_name} executed but returned no 'fields'.")
                        return [], []
                
                data_rows = result.get('data', [])
                all_data_rows.extend(data_rows)
                print(f"Fetched {len(data_rows)} records for {date_str}.")

                # Respect 1-minute rate limit
                if i < len(dates_to_process) - 1:
                    print("Waiting 65 seconds to respect report rate limit...")
                    time.sleep(65)

            except Exception as e:
                print(f"Failed to execute or process {report_name} for date {date_str}: {e}")
                if i < len(dates_to_process) - 1:
                    print("Waiting 65 seconds after error before next attempt...")
                    time.sleep(65)

        return all_data_rows, report_fields
    
    def transform_report_data(self, report_rows: List[List], report_fields: List[Dict], report_start_date: date, report_end_date: date) -> List[Dict]:
        """
        Generic function to transform a report's array-of-arrays into a list of dictionaries
        with snake_case keys based on the field labels.
        
        Also adds the overall report_start_date and report_end_date to each record.
        """
        print(f"\nTransforming {len(report_rows)} total records...")
        transformed_data = []
        
        if not report_fields:
            print("No fields provided to transform_report_data. Aborting.")
            return []

        start_date_str = report_start_date.strftime('%Y-%m-%d')
        end_date_str = report_end_date.strftime('%Y-%m-%d')

        header_map = {}
        for i, field in enumerate(report_fields):
            label = field.get('label', f"column_{i}")
            s1 = label.replace('#', 'Number')
            s2 = re.sub(r'[^a-zA-Z0-9 ]', '', s1)
            s3 = s2.strip().lower()
            snake_case_key = re.sub(r'\s+', '_', s3)
            header_map[i] = snake_case_key
            
        print(f"Created header map: {header_map}")

        for row in report_rows:
            if len(row) != len(report_fields):
                print(f"Skipping row, length mismatch. Expected {len(report_fields)}, got {len(row)}: {row}")
                continue
            
            record = {}
            for i, value in enumerate(row):
                key = header_map[i]
                record[key] = value
            
            record['report_start_date'] = start_date_str
            record['report_end_date'] = end_date_str
            
            transformed_data.append(record)
            
        print(f"Transformation complete. {len(transformed_data)} records prepared for Databox.")
        return transformed_data


def push_to_databox(data: List[Dict], push_url: str):
    """
    Pushes the transformed data to a specific Databox API endpoint in chunks.
    Returns a list of ingestionIds.
    """
    if not data:
        print("No data to push to Databox.")
        return []
        
    print(f"Pushing {len(data)} records to {push_url} in chunks...")
    headers = {'Content-Type': 'application/json', 'x-api-key': DATABOX_API_KEY}
    ingestion_ids = []
    
    for i in range(0, len(data), DATABOX_PAYLOAD_LIMIT):
        chunk = data[i:i + DATABOX_PAYLOAD_LIMIT]
        chunk_number = int(i / DATABOX_PAYLOAD_LIMIT) + 1
        print(f"Pushing chunk {chunk_number} ({len(chunk)} records)...")
        
        payload = {"records": chunk}
        response = make_api_request("POST", push_url, headers=headers, json_payload=payload)

        if response is None or response.status_code != 200:
            print(f"Error pushing chunk {chunk_number} to Databox.")
            if response is not None:
                print(f"Status: {response.status_code}, Response: {response.text}")
            continue

        try:
            result = response.json()
            if result.get("message") == "Data ingestion request accepted" and result.get("ingestionId"):
                ingestion_id = result["ingestionId"]
                print(f"Databox accepted chunk. Ingestion ID: {ingestion_id}")
                ingestion_ids.append(ingestion_id)
            else:
                print(f"Error: Databox push for chunk {chunk_number} was not accepted.")
                print(f"Response: {json.dumps(result, indent=2)}")
        except json.JSONDecodeError:
            print(f"Error: Could not decode Databox response: {response.text}")

    return ingestion_ids

def verify_databox_ingestion(ingestion_ids: List[str], verify_url_template: str):
    """
    Checks the status of the Databox ingestion for a list of IDs.
    """
    if not ingestion_ids:
        print("No ingestion IDs to verify.")
        return

    print(f"Waiting {VERIFICATION_WAIT_TIME} seconds before verification...")
    time.sleep(VERIFICATION_WAIT_TIME)
    
    headers = {'x-api-key': DATABOX_API_KEY}
    
    for ingestion_id in ingestion_ids:
        print(f"Verifying Databox ingestion status for {ingestion_id}...")
        verify_url = verify_url_template.format(ingestion_id=ingestion_id)
        response = make_api_request("GET", verify_url, headers=headers)

        if response is None or response.status_code != 200:
            print(f"Error: Databox verification for {ingestion_id} failed.")
            continue

        try:
            result = response.json()
            status = result.get("status")
            if status == "success":
                print(f"\n--- INGESTION {ingestion_id} SUCCESSFUL ---")
            elif status == "processing":
                print(f"\n--- INGESTION {ingestion_id} STILL PROCESSING ---")
            else:
                print(f"\n--- INGESTION {ingestion_id} FAILED ---")
                print(f"Status: {status}")
                print(f"Errors: {json.dumps(result.get('errors', {}), indent=2)}")
        except json.JSONDecodeError:
            print(f"Error: Could not decode Databox verification response: {response.text}")

def make_api_request(method, url, headers, json_payload=None, params=None, max_retries=API_MAX_RETRIES):
    """
    Generic API request function, used for Databox pushes.
    """
    retries = 0
    backoff_time = API_INITIAL_BACKOFF
    
    while retries < max_retries:
        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, json=json_payload, params=params, timeout=30)
            else:
                response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", backoff_time))
                print(f"Rate limit hit. Waiting {wait_time}s")
                time.sleep(wait_time)
                backoff_time = min(backoff_time * 2, 60)
            elif response.status_code >= 500:
                print(f"Server error ({response.status_code}). Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
            else:
                print(f"Client error: {response.status_code}.")
                return response
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}. Retrying in {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)
        retries += 1
    print(f"Max retries ({max_retries}) exceeded for {url}.")
    return None

# --- NEW: Generic processing function ---
def process_single_report(
    st_client: ServiceTitan, 
    config: ReportConfig, 
    dates_to_process: List[date], 
    start_date_payload: date, 
    end_date_payload: date,
    task_number: int,
    total_tasks: int
):
    """
    Runs the full ETL process for a single, configured report.
    """
    print(f"\n========= STARTING: {config.name} (Task {task_number}/{total_tasks}) =========")
    
    try:
        report_rows, report_fields = st_client.fetch_report_data_for_range(
            config.st_endpoint, 
            config.body_template, 
            dates_to_process, 
            config.name
        )
        
        if report_rows and report_fields:
            transformed_data = st_client.transform_report_data(
                report_rows, 
                report_fields,
                start_date_payload,
                end_date_payload
            )
            if transformed_data:
                ingestion_ids = push_to_databox(transformed_data, config.databox_push_url)
                verify_databox_ingestion(ingestion_ids, config.databox_verify_url)
            else:
                print(f"{config.name} transformation resulted in no data.")
        else:
            print(f"No data fetched for {config.name}. Skipping transform and push.")
    
    except Exception as e:
        print(f"!!!!!! An uncaught error occurred during '{config.name}' processing: {e} !!!!!!")
        # This try/except ensures that if one report fails, 
        # the script will still attempt the next report.
    
    print(f"========= FINISHED: {config.name} (Task {task_number}/{total_tasks}) =========")


# --- main() function ---
def main():
    """
    Main function to run the ETL (Extract, Transform, Load) process.
    """
    print(f"Starting ServiceTitan -> Databox Dataset script at {TIME_BEGIN}...")
    
    st_client = ServiceTitan()
    
    if not st_client.get_access_token():
        print("Halting process: Initial authentication failed.")
        return

    # --- Get dates to process ---
    if USE_CUSTOM_RANGE:
        print(f"--- Using CUSTOM date range: {CUSTOM_START_DATE} to {CUSTOM_END_DATE} ---")
        delta = CUSTOM_END_DATE - CUSTOM_START_DATE
        dates_to_process = [CUSTOM_START_DATE + timedelta(days=i) for i in range(delta.days + 1)]
        report_start_for_payload = CUSTOM_START_DATE
        report_end_for_payload = CUSTOM_END_DATE
    else:
        today = date.today()
        dates_to_process = [today]
        report_start_for_payload = today
        report_end_for_payload = today
        print(f"--- Using DAILY sync date (today): {today.strftime('%Y-%m-%d')} ---")
    
    print(f"--- All records will be tagged with start: {report_start_for_payload.strftime('%Y-%m-%d')}, end: {report_end_for_payload.strftime('%Y-%m-%d')} ---")

    # --- NEW: Loop through configured reports and process them ---
    total_tasks = len(ALL_REPORTS)
    for i, report_config in enumerate(ALL_REPORTS):
        process_single_report(
            st_client=st_client,
            config=report_config,
            dates_to_process=dates_to_process,
            start_date_payload=report_start_for_payload,
            end_date_payload=report_end_for_payload,
            task_number=i + 1,
            total_tasks=total_tasks
        )

    print(f"\nProcess finished. Total time: {datetime.now() - TIME_BEGIN}")

if __name__ == "__main__":
    main()