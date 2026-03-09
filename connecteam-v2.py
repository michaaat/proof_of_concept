import requests
import json
import datetime
import time
import argparse
import sys
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from datetime import timezone, timedelta
    # Fallback for systems < 3.9 without backports
    class ZoneInfo(datetime.tzinfo):
        def __init__(self, key): 
            self.key = key
        def utcoffset(self, dt): return timedelta(hours=-8) # Rough static fall back if needed
        def dst(self, dt): return timedelta(0)

# --- Configuration ---
CONNECTEAM_API_KEY = "dfgdfgdfg123123"
CONNECTEAM_USERS_URL = "asdadsad1231231"
CONNECTEAM_TIMESHEET_URL = "asdasda123123"

# Date Configuration (Leave empty for Daily Sync: Today in Vancouver)
# Format: "YYYY-MM-DD"
MANUAL_START_DATE = "" 
MANUAL_END_DATE = ""

# Databox Configuration
DATABOX_DATASET_ID = "xxx"
DATABOX_API_KEY = "xxxxxx"
DATABOX_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/data"
DATABOX_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_BATCH_SIZE = 100

# Script Settings
MAX_RETRIES = 5
INITIAL_BACKOFF = 1 # Seconds

# Custom Field Mapping
FIELD_MAPPING = {
    17204718: "team",
    17204719: "department",
    17204720: "Role",
    17204721: "branch"
}

def get_nested(data, keys, default=None):
    """Safely retrieves a nested value."""
    temp = data
    for key in keys:
        if isinstance(temp, dict):
            temp = temp.get(key)
        else:
            return default
    return temp if temp is not None else default

def get_date_range():
    """
    Determines date range based on Configuration variables or defaults to Today (Vancouver).
    """
    # 1. Check Manual Configuration
    if MANUAL_START_DATE:
        start = MANUAL_START_DATE
        end = MANUAL_END_DATE if MANUAL_END_DATE else MANUAL_START_DATE
        print(f"Mode: Historical Sync (Configured: {start} to {end})")
        return start, end

    # 2. Check CLI Arguments (Optional Override)
    parser = argparse.ArgumentParser(description="Connecteam to Databox ETL")
    parser.add_argument("--start-date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD")
    args, _ = parser.parse_known_args() # Use parse_known_args to avoid interfering if run in weird envs

    if args.start_date:
        start = args.start_date
        end = args.end_date if args.end_date else args.start_date
        print(f"Mode: Historical Sync (CLI: {start} to {end})")
        return start, end
    
    # 3. Default: Today in Vancouver
    try:
        # Try proper timezone
        tz = ZoneInfo("America/Vancouver")
        now_van = datetime.datetime.now(tz)
    except Exception:
        # Fallback (UTC-8)
        tz = datetime.timezone(datetime.timedelta(hours=-8))
        now_van = datetime.datetime.now(tz)
        
    today_str = now_van.strftime("%Y-%m-%d")
    print(f"Mode: Daily Sync (Today: {today_str} [America/Vancouver])")
    return today_str, today_str

# --- API Helper with Rate Limiting & Backoff ---

def make_request(method, url, headers, params=None, json_data=None):
    """
    Executes an API request with retry logic, exponential backoff, 
    and respect for Connecteam/Databox rate limits.
    """
    retries = 0
    backoff = INITIAL_BACKOFF
    
    while retries < MAX_RETRIES:
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params, timeout=30)
            else:
                response = requests.post(url, headers=headers, json=json_data, timeout=30)
            
            # 1. Connecteam Rate Limit Handling
            if "x-ratelimit-minute-remaining" in response.headers:
                remaining = int(response.headers.get("x-ratelimit-minute-remaining", 100))
                if remaining < 3:
                     reset_epoch = response.headers.get("x-ratelimit-minute-reset")
                     if reset_epoch:
                         wait_s = float(reset_epoch) - time.time()
                         wait_s = max(wait_s, 2)
                         print(f"[RateLimit] Connecteam limit approaching. Sleeping {wait_s:.2f}s...")
                         time.sleep(wait_s)
                     else:
                         print("[RateLimit] Connecteam limit approaching. Short pause...")
                         time.sleep(5)

            # 2. Status Code Handling
            if response.status_code == 200:
                return response
            
            elif response.status_code == 429:
                # Rate Limit Hit
                retry_after = response.headers.get("Retry-After")
                wait_time = int(retry_after) if retry_after else backoff
                print(f"[429] Rate Limit Hit. Waiting {wait_time}s...")
                time.sleep(wait_time)
                backoff *= 2
            
            elif response.status_code >= 500:
                print(f"[{response.status_code}] Server Error. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
            
            else:
                print(f"[{response.status_code}] Client Error: {response.text}")
                return response 

        except requests.exceptions.RequestException as e:
            print(f"[Exception] Network error: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)
            backoff *= 2
        
        retries += 1
    
    print(f"Max retries ({MAX_RETRIES}) exceeded for {url}")
    return None

# --- Core Logic ---

def fetch_users_map():
    headers = {
        "X-API-KEY": CONNECTEAM_API_KEY,
        "Content-Type": "application/json"
    }
    
    limit = 500
    offset = 0
    all_users = []
    
    print("Fetching Users...")
    
    while True:
        params = {"limit": limit, "offset": offset}
        response = make_request("GET", CONNECTEAM_USERS_URL, headers, params=params)
        
        if not response or response.status_code != 200:
            print("Failed to fetch users batch.")
            break
            
        data = response.json()
        users_batch = data.get("data", {}).get("users", [])
        
        if not users_batch:
            break
            
        all_users.extend(users_batch)
        print(f"  Fetched batch (offset {offset}). Total so far: {len(all_users)}")
        
        if len(users_batch) < limit:
            break
            
        offset += limit
        
    # Process Map
    users_map = {}
    for user in all_users:
        user_id = user.get("userId")
        user_data = {
            "firstName": user.get("firstName"),
            "lastName": user.get("lastName"),
            "team": None,
            "department": None,
            "Role": None,
            "branch": None
        }
        
        custom_fields = user.get("customFields", [])
        for field in custom_fields:
            field_id = field.get("customFieldId")
            if field_id in FIELD_MAPPING:
                target_key = FIELD_MAPPING[field_id]
                field_val = field.get("value")
                
                if isinstance(field_val, list):
                    extracted = [str(item.get("value")) for item in field_val if isinstance(item, dict)]
                    user_data[target_key] = ", ".join(extracted) if extracted else str(field_val)
                elif isinstance(field_val, dict):
                    user_data[target_key] = str(field_val.get("value", field_val))
                else:
                    user_data[target_key] = str(field_val)
        
        users_map[user_id] = user_data
    
    print(f"Total Users Map Size: {len(users_map)}")
    return users_map

def fetch_timesheets(start_date, end_date):
    headers = {
        "X-API-KEY": CONNECTEAM_API_KEY,
        "Content-Type": "application/json"
    }
    
    # Pagination
    limit = 50
    offset = 0
    all_timesheet_users = []
    root_metadata = {}
    
    print(f"Fetching Timesheets ({start_date} to {end_date})...")
    
    while True:
        params = {
            "startDate": start_date,
            "endDate": end_date,
            "limit": limit,
            "offset": offset
        }
        
        response = make_request("GET", CONNECTEAM_TIMESHEET_URL, headers, params=params)
        
        if not response or response.status_code != 200:
            print("Failed to fetch timesheets batch.")
            break
            
        data = response.json()
        data_content = data.get("data", {})
        
        # Check for empty data on first batch or overall
        users_batch = data_content.get("users", [])
        
        if offset == 0 and not users_batch:
            print("No users found in timesheet data for this period.")
            return None # Signal empty
            
        if not root_metadata:
            root_metadata["startDate"] = data_content.get("startDate")
            root_metadata["endDate"] = data_content.get("endDate")
            
        if not users_batch:
            break
            
        all_timesheet_users.extend(users_batch)
        print(f"  Fetched batch (offset {offset}). Users with records: {len(users_batch)}")
        
        if len(users_batch) < limit:
            break
            
        offset += limit
        
    return {"users": all_timesheet_users, "metadata": root_metadata}

def process_and_merge(timesheet_raw, users_map):
    print("Processing and Merging Data...")
    merged_data = []
    
    if not timesheet_raw or not timesheet_raw.get("users"):
        return []

    root_start_date = timesheet_raw["metadata"].get("startDate")
    root_end_date = timesheet_raw["metadata"].get("endDate")
    users_ts = timesheet_raw["users"]
    
    for user_ts in users_ts:
        user_id = user_ts.get("userId")
        daily_records = user_ts.get("dailyRecords", [])
        
        user_details = users_map.get(user_id, {})
        
        for daily in daily_records:
            base_record = {
                "startDate": root_start_date,
                "endDate": root_end_date,
                "userId": user_id,
                "date": daily.get("date"),
                "dailyTotalHours": daily.get("dailyTotalHours", 0.0),
                "dailyTotalWorkHours": daily.get("dailyTotalWorkHours", 0.0),
                "dailyTotalPaidBreakHours": daily.get("dailyTotalPaidBreakHours", 0.0),
                "dailyTotalUnpaidBreakHours": daily.get("dailyTotalUnpaidBreakHours", 0.0),
                "isApproved": daily.get("isApproved"),
                "isSubmitted": daily.get("isSubmitted"),
                "isLocked": daily.get("isLocked"),
                
                "firstName": user_details.get("firstName"),
                "lastName": user_details.get("lastName"),
                "fullName": f"{user_details.get('firstName') or ''} {user_details.get('lastName') or ''}".strip(),
                "team": user_details.get("team"),
                "department": user_details.get("department"),
                "Role": user_details.get("Role"),
                "branch": user_details.get("branch")
            }
            
            pay_items = daily.get("payItems", [])
            
            if not pay_items:
                record = base_record.copy()
                record.update({
                    "hours": 0.0,
                    "type": None,
                    "actualPayRate": 0.0,
                    "totalPay": 0.0
                })
                merged_data.append(record)
            else:
                for item in pay_items:
                    record = base_record.copy()
                    record.update({
                        "hours": item.get("hours", 0.0),
                        "type": get_nested(item, ["payRule", "type"]),
                        "actualPayRate": item.get("actualPayRate", 0.0),
                        "totalPay": item.get("totalPay", 0.0)
                    })
                    merged_data.append(record)
                    
    return merged_data

def verify_ingestion(ingestion_id):
    """Checks the status of a specific ingestion ID."""
    url = DATABOX_VERIFY_URL_TEMPLATE.format(ingestion_id=ingestion_id)
    headers = {
        "x-api-key": DATABOX_API_KEY,
        "Accept": "application/vnd.databox.v1+json" 
    }
    
    print(f"  Verifying ID: {ingestion_id}...")
    
    for _ in range(5):
        response = make_request("GET", url, headers)
        
        if response and response.status_code == 200:
            data = response.json()
            status = data.get("status", "").lower()
            
            if status in ["processed", "success", "in progress", "inprogress"]:
                print(f"    Status: {status} (OK)")
                return True
            elif status == "failed":
                errors = data.get("errors")
                print(f"    Status: {status}. Errors: {errors}")
                return False
            else:
                print(f"    Status: {status}...")
        
        time.sleep(2)
            
    print("    Verification timed out. Continuing (check logs if needed).")
    return True

def push_to_databox(data):
    if not data:
        print("No data to push.")
        return

    print(f"Pushing {len(data)} records to Databox...")
    
    headers = {
        "x-api-key": DATABOX_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/vnd.databox.v1+json"
    }
    
    total_batches = (len(data) + DATABOX_BATCH_SIZE - 1) // DATABOX_BATCH_SIZE
    
    for i in range(0, len(data), DATABOX_BATCH_SIZE):
        batch = data[i:i + DATABOX_BATCH_SIZE]
        batch_num = (i // DATABOX_BATCH_SIZE) + 1
        
        print(f"--- Batch {batch_num}/{total_batches} ({len(batch)} records) ---")
        
        payload = {"records": batch}
        
        response = make_request("POST", DATABOX_PUSH_URL, headers, json_data=payload)
        
        if response and response.status_code == 200:
            res_json = response.json()
            ingestion_id = res_json.get("ingestionId") or res_json.get("id")
            
            if ingestion_id:
                is_ok = verify_ingestion(ingestion_id)
                if not is_ok:
                    print("CRITICAL: Ingestion Failed. Stopping script.")
                    break
            else:
                print(f"  Warning: Push OK but no ID found. Response: {res_json}")
        else:
            print("CRITICAL: Batch push failed. Stopping script.")
            break

def main():
    # 1. Determine Dates
    start_date, end_date = get_date_range()
    
    # 2. Fetch Timesheets FIRST (Fail fast if no data)
    ts_raw = fetch_timesheets(start_date, end_date)
    
    if not ts_raw:
        print("Script finished: No timesheet data to process.")
        return

    # 3. Fetch Users (Only if we have timesheets)
    users_map = fetch_users_map()
    
    # 4. Merge
    final_payload = process_and_merge(ts_raw, users_map)
    
    # 5. Push
    if final_payload:
        push_to_databox(final_payload)
    else:
        print("No data merged after processing. Nothing to push.")

if __name__ == "__main__":
    main()
