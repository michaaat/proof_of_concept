import requests
import json
import os
import time
import datetime
import pytz
from requests.auth import HTTPBasicAuth

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# --- 📅 SYNC SETTINGS ---
# Set a string "YYYY-MM-DD" to sync from a specific date backwards until that date ends.
# Set to None to automatically use "Today" (Asia/Manila time).
MANUAL_SYNC_DATE = "2025-12-12" 

# --- Freshcaller Configuration ---
FC_DOMAIN = "https://xxxxxxx.freshcaller.com"
FC_API_KEY = "3bxxxx"
FC_HEADERS = {
    'accept': 'application/json',
    'X-Api-Auth': FC_API_KEY
}

# --- Databox Configuration ---
DATABOX_TOKEN = "pak_xxxx"
DATABOX_DATASET_ID = "9e8xxxx"
DATABOX_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/data"
DATABOX_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'x-api-key': f'{DATABOX_TOKEN}' 
}

BATCH_SIZE = 100

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

def get_target_cutoff_date():
    """Determines the cutoff date based on manual config or Manila time."""
    tz = pytz.timezone('Asia/Manila')
    
    if MANUAL_SYNC_DATE:
        target_str = MANUAL_SYNC_DATE
        print(f"📅 Manual Sync Mode: Stopping when calls are older than {target_str}")
    else:
        now_manila = datetime.datetime.now(tz)
        target_str = now_manila.strftime("%Y-%m-%d")
        print(f"📅 Auto Sync Mode (Asia/Manila): Stopping when calls are older than {target_str}")
    
    return target_str

def check_freshcaller_limits(response):
    """
    Monitors Freshcaller Rate Limits headers.
    Sleeps if remaining requests are critically low.
    """
    try:
        remaining = int(response.headers.get('X-RateLimit-Remaining', 100))
        
        # If we have less than 10 requests left, sleep for a minute to reset
        if remaining < 10:
            print(f"⚠️ Freshcaller Rate Limit Low ({remaining} remaining). Sleeping 60s...")
            time.sleep(60)
    except Exception as e:
        pass # Headers might not exist in error states

def fetch_call_metrics(call_id):
    """
    Fetches the detailed metrics for a specific call ID.
    """
    url = f"{FC_DOMAIN}/api/v1/calls/{call_id}/call_metrics"
    try:
        response = requests.get(url, headers=FC_HEADERS)
        check_freshcaller_limits(response)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('call_metrics', {})
        elif response.status_code == 429:
            print("⚠️ Hit Rate Limit on Metrics fetch. Sleeping 60s...")
            time.sleep(60)
            return fetch_call_metrics(call_id) # Retry
        else:
            print(f"⚠️ Failed to fetch metrics for {call_id}: {response.status_code}")
            return {}
    except Exception as e:
        print(f"❌ Error fetching metrics: {e}")
        return {}

def transform_data(call_data, metrics_data):
    """
    Merges Call Data + Metrics Data + Flattens Objects
    """
    # 1. Base Fields from Call List
    payload = {
        # Databox requires a 'date' field to map the data timeline
        "date": call_data.get('created_time'), 
        "id": str(call_data.get('id')),
        "direction": call_data.get('direction'),
        "assigned_agent_name": call_data.get('assigned_agent_name'),
        "bill_duration": float(call_data.get('bill_duration') or 0),
        "bill_duration_unit": call_data.get('bill_duration_unit'),
        "updated_time": call_data.get('updated_time')
    }

    # 2. Extract Participant Info (Focus on Customer)
    participants = call_data.get('participants', [])
    # Find the customer participant, otherwise fallback to the first one
    customer_part = next((p for p in participants if p.get('participant_type') == 'Customer'), None)
    
    if customer_part:
        payload["participant_type"] = "Customer"
        payload["caller_name"] = customer_part.get('caller_name')
        payload["caller_number"] = customer_part.get('caller_number')
    elif participants:
        # Fallback if no specific 'Customer' type found
        payload["participant_type"] = participants[0].get('participant_type')
        payload["caller_name"] = participants[0].get('caller_name')
        payload["caller_number"] = participants[0].get('caller_number')

    # 3. Merge Metrics Data
    if metrics_data:
        # Add direct mappings
        fields_to_map = [
            "ivr_time", "hold_duration", "call_work_time", 
            "total_ringing_time", "talk_time", "answering_speed", 
            "recording_duration", "cost"
        ]
        
        for field in fields_to_map:
            # Convert to float/int for Databox metrics, handle None as 0
            val = metrics_data.get(field)
            payload[field] = val if val is not None else 0

        # Flatten CSAT
        csat = metrics_data.get('csat', {})
        if csat:
            payload["csat_transfer_made"] = str(csat.get('transfer_made', False))
            payload["csat_outcome"] = str(csat.get('outcome', ''))
            payload["csat_time"] = csat.get('time', 0)

    return payload

def verify_ingestion(ingestion_id):
    """
    Checks the status of the ingestion ID with Databox
    """
    if not ingestion_id:
        return

    # Wait a moment for Databox to process (async processing)
    time.sleep(3) 
    
    verify_url = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/ingestions/{ingestion_id}"
    
    try:
        response = requests.get(verify_url, headers=DATABOX_HEADERS)
        if response.status_code == 200:
            status_data = response.json()
            state = status_data.get("status", "unknown")
            print(f"🔎 Validation Status: {state.upper()}")
            
            # Print errors if it failed
            if state == "failed":
                print(f"⚠️ Errors: {status_data.get('errors')}")
        else:
            print(f"⚠️ Could not verify ingestion: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Verification error: {e}")

def push_batch_to_databox(batch_data):
    """Pushes a list of records to Databox and returns Ingestion ID"""
    if not batch_data:
        return None

    payload = { "records": batch_data }
    
    try:
        response = requests.post(DATABOX_PUSH_URL, headers=DATABOX_HEADERS, json=payload)
        
        if response.status_code == 200:
            res_json = response.json()
            # Capture ID for validation
            ingestion_id = res_json.get("id") or res_json.get("ingestionId")
            print(f"✅ Successfully pushed {len(batch_data)} records to Databox. ID: {ingestion_id}")
            return ingestion_id
        else:
            print(f"❌ Databox Push Failed ({response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error pushing to Databox: {e}")
        return None

# ==========================================
# 🚀 MAIN EXECUTION
# ==========================================

def main():
    cutoff_date_str = get_target_cutoff_date()
    keep_fetching = True
    page = 1
    
    processed_batch = []
    total_processed = 0

    print("🚀 Starting Freshcaller to Databox Sync...")

    while keep_fetching:
        print(f"📄 Fetching List Page {page}...")
        
        # Fetch List of Calls
        list_url = f"{FC_DOMAIN}/api/v1/calls?per_page=50&page={page}"
        response = requests.get(list_url, headers=FC_HEADERS)
        check_freshcaller_limits(response)

        if response.status_code != 200:
            print(f"❌ Error fetching call list: {response.status_code}")
            break

        data = response.json()
        calls = data.get('calls', [])
        meta = data.get('meta', {})

        if not calls:
            print("⚠️ No calls found on this page.")
            break

        # Iterate through calls on this page
        for call in calls:
            # 1. Check Date Stopper
            call_date_str = call.get('created_time', '')[:10]
            
            if call_date_str < cutoff_date_str:
                print(f"🛑 Found date ({call_date_str}) older than cutoff ({cutoff_date_str}). Stopping execution.")
                keep_fetching = False
                break
            
            # 2. Process Valid Call (Fetch Metrics)
            print(f"   🔍 Processing Call ID: {call['id']} ({call_date_str})")
            
            # Fetch detailed metrics (The Lookup)
            metrics = fetch_call_metrics(call['id'])
            
            # Transform and Merge
            enriched_record = transform_data(call, metrics)
            processed_batch.append(enriched_record)
            
            # 3. Push to Databox if Batch is full
            if len(processed_batch) >= BATCH_SIZE:
                ingestion_id = push_batch_to_databox(processed_batch)
                verify_ingestion(ingestion_id) 
                total_processed += len(processed_batch)
                processed_batch = []

        # Check pagination
        if keep_fetching:
            if page >= meta.get('total_pages', 0):
                print("⚠️ Reached last page.")
                keep_fetching = False
            else:
                page += 1

    # Push remaining records
    if processed_batch:
        ingestion_id = push_batch_to_databox(processed_batch)
        verify_ingestion(ingestion_id) 
        total_processed += len(processed_batch)

    print(f"\n🎉 Sync Complete. Total Records Processed: {total_processed}")

if __name__ == "__main__":
    main()