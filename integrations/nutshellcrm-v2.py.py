import requests
import datetime
import time
import json

# --- CONFIGURATION ---
NUTSHELL_AUTH_HEADER = "Basic aGVsbadasdasdadqwesad"
NUTSHELL_SOURCES_URL = "https://app.nutshell.com/rest/sources"
NUTSHELL_LEADS_URL = "https://app.nutshell.com/rest/leads"

DATABOX_API_KEY = "pak_204asdasdadasd"
DATABOX_DATASET_ID = "ac294zxczczxcxz"
DATABOX_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/data"
DATABOX_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/ingestions/{{ingestion_id}}"

# OPTIMIZATION: Set how many days of history you want to sync per run.
# 30 days is standard to catch new leads and recent late stage closures.
DAYS_TO_SYNC = 1000

VERIFICATION_WAIT_TIME = 5
DATABOX_MAX_RETRIES = 3
DATABOX_INITIAL_BACKOFF = 1

def get_headers():
    return {
        'accept': 'application/json',
        'authorization': NUTSHELL_AUTH_HEADER
    }

def fetch_nutshell_sources_map():
    """
    Fetches all sources and returns a dictionary mapping ID -> {'name': Source, 'channel': Channel Display}.
    """
    print("Fetching Sources map from Nutshell...")
    try:
        response = requests.get(NUTSHELL_SOURCES_URL, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            source_map = {}
            for item in data.get('sources', []):
                # Initialize with the specific source name
                source_data = {'name': item.get('name')}
                
                # Check for high-level channel
                if item.get('channel') and isinstance(item['channel'], dict) and item['channel'].get('display'):
                     source_data['channel'] = item['channel']['display']
                else:
                    source_data['channel'] = None # Explicitly set to None if no channel exists
                    
                source_map[item['id']] = source_data
            
            print(f"Mapped {len(source_map)} sources.")
            return source_map
        else:
            print(f"Error fetching sources: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Exception fetching sources: {e}")
        return {}

def fetch_all_nutshell_leads(source_map, days_to_sync):
    """
    Paginate through Nutshell leads newest-first until we hit leads older than days_to_sync.
    """
    all_records = []
    page = 1
    
    # Calculate the unix timestamp for our cutoff point
    cutoff_timestamp = time.time() - (days_to_sync * 86400)
    
    print(f"Starting Lead extraction. Looking back {days_to_sync} days...")

    while True:
        print(f"Fetching Nutshell Page {page}...")
        
        params = {
            "page[page]": page,
            # "sort": "-createdTime" tells the API to bring newest leads first
            "sort": "-createdTime" 
        }
        
        try:
            response = requests.get(NUTSHELL_LEADS_URL, headers=get_headers(), params=params, timeout=60)
            
            if response.status_code != 200:
                print(f"Error on page {page}: {response.status_code}")
                break
                
            data = response.json()
            leads = data.get('leads', [])
            
            if not leads:
                print("No more leads found. Extraction complete.")
                break
            
            reached_cutoff = False
            
            for lead in leads:
                # Get the created timestamp
                created_ts = lead.get('createdTime', {}).get('timestamp', 0)
                
                # Check if this lead is older than our cutoff date
                if created_ts and created_ts < cutoff_timestamp:
                    reached_cutoff = True
                    break # Stop processing this page, we went too far back
                    
                record = transform_single_lead(lead, source_map)
                all_records.append(record)
            
            # If we broke the for-loop above, it means we reached old data and can stop requesting pages
            if reached_cutoff:
                print(f"Reached leads older than {days_to_sync} days. Stopping pagination.")
                break
            
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Exception during pagination: {e}")
            break
            
    return all_records

def transform_single_lead(lead, source_map):
    """
    Extracts specific fields, converts timestamps, and flattens both Sources and Channels into separate columns.
    Ensures value_amount is a number.
    """
    created_ts = lead.get('createdTime', {}).get('timestamp')
    if created_ts:
        date_str = datetime.datetime.fromtimestamp(created_ts).strftime('%Y-%m-%d %H:%M:%S')
    else:
        date_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    source_ids = lead.get('links', {}).get('sources', [])
    
    # Resolve IDs to get raw lists of names and channels
    raw_source_names = [source_map.get(sid, {}).get('name', sid) for sid in source_ids if source_map.get(sid)]
    raw_channels = [source_map.get(sid, {}).get('channel') for sid in source_ids if source_map.get(sid)]
    
    # Remove nulls from channels list before deduplication
    raw_channels = [ch for ch in raw_channels if ch is not None]

    # Dedup while preserving order
    resolved_sources = list(dict.fromkeys(raw_source_names))
    resolved_channels = list(dict.fromkeys(raw_channels))
    
    # Create fields for sources (e.g., "google", "bing")
    source_fields = {}
    for i in range(5):
        key = f"source_{i+1}"
        if i < len(resolved_sources):
            source_fields[key] = resolved_sources[i]
        else:
            source_fields[key] = None 
            
    # Create fields for channels (e.g., "Paid Search", "Organic Social")
    channel_fields = {}
    for i in range(5):
        key = f"channel_{i+1}"
        if i < len(resolved_channels):
            channel_fields[key] = resolved_channels[i]
        else:
            channel_fields[key] = None 

    # Ensure numeric types and add null safety for Databox
    numeric_amount = float((lead.get("value") or {}).get("amount") or 0)
    numeric_confidence = float(lead.get("confidence") or 0)

    record = {
        "date": date_str,
        "id": lead.get("id"),
        "lead_name": lead.get("name"),
        "lead_number": lead.get("number"),
        "status": lead.get("status"),
        "confidence": numeric_confidence,
        "value_amount": numeric_amount,
        "owner_id": lead.get("links", {}).get("owner"),
        **source_fields,
        **channel_fields
    }
    
    if lead.get('closedTime'):
        record['closed_date'] = datetime.datetime.fromtimestamp(lead['closedTime']['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
    
    if lead.get('dueTime'):
        record['due_date'] = datetime.datetime.fromtimestamp(lead['dueTime']['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
        
    if lead.get('anticipatedClosedTime'):
        record['anticipated_closed_date'] = datetime.datetime.fromtimestamp(lead['anticipatedClosedTime']['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

    return record
  
def make_databox_request(method, url, headers, json_payload=None, max_retries=DATABOX_MAX_RETRIES):
    retries = 0
    backoff = DATABOX_INITIAL_BACKOFF
    
    while retries < max_retries:
        try:
            if method == "POST":
                r = requests.post(url, headers=headers, json=json_payload, timeout=30)
            else:
                r = requests.get(url, headers=headers, timeout=30)
            
            if r.status_code == 200:
                return r
            elif r.status_code == 429:
                print(f"Rate limit. Waiting {backoff}s")
                time.sleep(backoff)
                backoff *= 2
            else:
                print(f"Databox Error {r.status_code}: {r.text}")
                return None
        except Exception as e:
            print(f"Request failed: {e}")
            time.sleep(backoff)
            backoff *= 2
        
        retries += 1
    return None

def push_to_databox(data):
    if not data:
        return None
        
    print(f"Pushing {len(data)} records to Databox...")
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': DATABOX_API_KEY
    }
    
    chunk_size = 100
    last_ingestion_id = None
    
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        payload = {"records": chunk} 
        
        print(f"Sending chunk {i} to {i+len(chunk)}...")
        response = make_databox_request("POST", DATABOX_PUSH_URL, headers=headers, json_payload=payload)
        
        if response and response.status_code == 200:
            res_json = response.json()
            last_ingestion_id = res_json.get("ingestionId")
            print(f"Chunk accepted. ID: {last_ingestion_id}")
        else:
            print("Chunk failed.")
            return None
            
    return last_ingestion_id

def verify_databox_ingestion(ingestion_id):
    if not ingestion_id:
        return
    print(f"Waiting {VERIFICATION_WAIT_TIME}s before verification...")
    time.sleep(VERIFICATION_WAIT_TIME)
    
    url = DATABOX_VERIFY_URL_TEMPLATE.format(ingestion_id=ingestion_id)
    headers = {'x-api-key': DATABOX_API_KEY}
    
    response = make_databox_request("GET", url, headers=headers)
    if response:
        print(f"Ingestion Status: {response.text}")

def main():
    source_map = fetch_nutshell_sources_map()
    
    leads_data = fetch_all_nutshell_leads(source_map, DAYS_TO_SYNC)
    
    print(f"Total extracted and transformed leads: {len(leads_data)}")
    
    if leads_data:
        ingestion_id = push_to_databox(leads_data)
        verify_databox_ingestion(ingestion_id)
    else:
        print("No data to push.")

if __name__ == "__main__":
    main()