import requests
import datetime
import time
import json

# --- CONFIGURATION ---
NUTSHELL_AUTH_HEADER = "Basic aGVsxxxxxx"
NUTSHELL_SOURCES_URL = "https://app.nutshell.com/rest/sources"
NUTSHELL_LEADS_URL = "https://app.nutshell.com/rest/leads"

DATABOX_API_KEY = "pak_xxx"
DATABOX_DATASET_ID = "axx"
DATABOX_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/data"
DATABOX_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_DATASET_ID}/ingestions/{{ingestion_id}}"

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
    Fetches all sources and returns a dictionary mapping ID -> Channel Display.
    """
    print("Fetching Sources map from Nutshell...")
    try:
        response = requests.get(NUTSHELL_SOURCES_URL, headers=get_headers(), timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            source_map = {}
            for item in data.get('sources', []):
                if item.get('channel') and isinstance(item['channel'], dict) and item['channel'].get('display'):
                     source_map[item['id']] = item['channel']['display']
                else:
                    # Fallback to specific name if channel is missing (e.g., "Cold Call" or "Web Signup")
                    source_map[item['id']] = item['name']
            
            print(f"Mapped {len(source_map)} sources to their Channels.")
            return source_map
        else:
            print(f"Error fetching sources: {response.status_code}")
            return {}
    except Exception as e:
        print(f"Exception fetching sources: {e}")
        return {}

def fetch_all_nutshell_leads(source_map):
    """
    Paginate through Nutshell leads until no data is returned.
    Transform them immediately to prepare for Databox.
    """
    all_records = []
    page = 1
    limit = 500 
    
    print("Starting Lead extraction...")

    while True:
        print(f"Fetching Nutshell Page {page}...")
        
        params = {
            "page[page]": page,
            "page[limit]": limit
        }
        
        try:
            response = requests.get(NUTSHELL_LEADS_URL, headers=get_headers(), params=params, timeout=60)
            
            if response.status_code != 200:
                print(f"Error on page {page}: {response.status_code}")
                break
                
            data = response.json()
            leads = data.get('leads', [])
            
            # STOP CONDITION: If list is empty, we are done
            if not leads:
                print("No more leads found. Extraction complete.")
                break
            
            # Transform this batch
            for lead in leads:
                record = transform_single_lead(lead, source_map)
                all_records.append(record)
            
            # Increment page for next loop
            page += 1
            
            # Optional: Sleep briefly to be nice to the API
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Exception during pagination: {e}")
            break
            
    return all_records

def transform_single_lead(lead, source_map):
    """
    Extracts specific fields, converts timestamps, and flattens sources using Channel names.
    Validates uniqueness: Removes duplicate sources for the same lead.
    """
    # 1. Handle Timestamp (Use createdTime as the primary 'date' for the row)
    created_ts = lead.get('createdTime', {}).get('timestamp')
    if created_ts:
        date_str = datetime.datetime.fromtimestamp(created_ts).strftime('%Y-%m-%d %H:%M:%S')
    else:
        date_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # 2. Handle Sources (Map ID -> Channel Name, then Flatten & Dedup)
    source_ids = lead.get('links', {}).get('sources', [])
    
    # Resolve IDs to Channel Names using the map we built
    raw_sources = [source_map.get(sid, sid) for sid in source_ids]
    
    # Using dict.fromkeys() is the fastest way to dedup and keep order in Python
    resolved_sources = list(dict.fromkeys(raw_sources))
    
    # Create fields source_1, source_2, source_3...
    source_fields = {}
    for i in range(5): # Support up to 5 unique sources per lead
        key = f"source_{i+1}"
        if i < len(resolved_sources):
            source_fields[key] = resolved_sources[i]
        else:
            source_fields[key] = None # Leave empty if no source

    # 3. Build the final record
    record = {
        "date": date_str,
        "id": lead.get("id"),
        "lead_name": lead.get("name"),
        "lead_number": lead.get("number"),
        "status": lead.get("status"),
        "confidence": lead.get("confidence"),
        "value_amount": lead.get("value", {}).get("amount", 0),
        "owner_id": lead.get("links", {}).get("owner"),
        # Add the flattened sources
        **source_fields
    }
    
    # Add other timestamps if they exist
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
    """
    Pushes data in chunks.
    """
    if not data:
        return None
        
    print(f"Pushing {len(data)} records to Databox...")
    
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': DATABOX_API_KEY
    }
    
    # Chunk size 100
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
    # 1. Fetch Source Mapping (7-sources -> Paid Search)
    source_map = fetch_nutshell_sources_map()
    
    # 2. Fetch Leads (Paginated) and Transform
    leads_data = fetch_all_nutshell_leads(source_map)
    
    print(f"Total extracted and transformed leads: {len(leads_data)}")
    
    # 3. Push to Databox
    if leads_data:
        ingestion_id = push_to_databox(leads_data)
        
        # 4. Verify
        verify_databox_ingestion(ingestion_id)
    else:
        print("No data to push.")

if __name__ == "__main__":
    main()