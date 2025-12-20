import requests
import datetime
import pytz
import time
import json

# --- Configuration ---

# Permaleads Credentials
PERMALEADS_EMAIL = "ag-xxxxx"
PERMALEADS_PASSWORD = "xxxxx"
PERMALEADS_WEBSITE_ID = "0a960xxxxx"

# Base URLs
PERMALEADS_AUTH_URL = "https://api.my.permaleads.ch/v1/auth/login"
PERMALEADS_REPORT_URL = "https://api.my.permaleads.ch/v1/visits/report/daily"

# --- Databox Configuration ---
DATABOX_API_KEY = "pak_cxxxx"
DATABOX_PAYLOAD_LIMIT = 100 

# Dataset 1: Leads
DATABOX_LEADS_ID = "b64xxxx"
DATABOX_LEADS_URL = f"https://api.databox.com/v1/datasets/{DATABOX_LEADS_ID}/data"

# Dataset 2: Categories
DATABOX_CATS_ID = "01xxxx"
DATABOX_CATS_URL = f"https://api.databox.com/v1/datasets/{DATABOX_CATS_ID}/data"

# Dataset 3: Visits (Views)
DATABOX_VISITS_ID = "a2xxxx"
DATABOX_VISITS_URL = f"https://api.databox.com/v1/datasets/{DATABOX_VISITS_ID}/data"

# --- Script Configuration ---
TIMEZONE = "Europe/Zurich" 
API_MAX_RETRIES = 3
API_INITIAL_BACKOFF = 1 

# --- Helper Functions ---

def get_yesterday_date_str():
    """
    Returns yesterday's date in YYYY-MM-DD format (Europe/Zurich time).
    """
    try:
        tz = pytz.timezone(TIMEZONE)
        # Get 'now' in the target timezone
        now_local = datetime.datetime.now(tz)
        # Subtract one day
        yesterday = now_local - datetime.timedelta(days=1)
        # Return strict YYYY-MM-DD string
        return yesterday.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Timezone error: {e}. Defaulting to UTC yesterday.")
        return (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

def make_api_request(method, url, headers, json_payload=None, max_retries=API_MAX_RETRIES):
    retries = 0
    backoff_time = API_INITIAL_BACKOFF
    
    while retries < max_retries:
        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, json=json_payload, timeout=30)
            else:
                response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                return response 
            elif response.status_code >= 500:
                print(f"Server error ({response.status_code}). Retrying...")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
            else:
                print(f"Client error: {response.status_code}. Response: {response.text}")
                return response 

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying...")
            time.sleep(backoff_time)
        
        retries += 1
    return None

# --- Authentication ---

def authenticate_permaleads():
    print("Authenticating with Permaleads...")
    headers = {'Content-Type': 'application/json'}
    payload = {
        "email": PERMALEADS_EMAIL,
        "password": PERMALEADS_PASSWORD
    }
    
    response = make_api_request("POST", PERMALEADS_AUTH_URL, headers, payload)
    
    if response and response.status_code == 200:
        data = response.json()
        if data.get("success"):
            print("Authentication successful.")
            return data.get("token")
    
    print("Authentication failed.")
    return None

def fetch_daily_report(token, report_date):
    print(f"Fetching report for date: {report_date}")
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}" 
    }
    
    # API requires STRICT "YYYY-MM-DD"
    payload = {
        "date": report_date,
        "website": PERMALEADS_WEBSITE_ID
    }
    
    response = make_api_request("POST", PERMALEADS_REPORT_URL, headers, payload)
    
    if response and response.status_code == 200:
        data = response.json()
        if data.get("success"):
            return data.get("leads", [])
    
    print("Failed to fetch report data or Success=False.")
    return []

# --- Transformations ---

def transform_leads(leads_data, report_date):
    print("Transforming LEADS data...")
    transformed = []
    
    for lead in leads_data:
        # Flatten Address
        address = lead.get("address", {})
        
        # Flatten Registration (Take first item if exists)
        reg_list = lead.get("registrationNumbers", [])
        reg_data = reg_list[0] if reg_list else {}

        # --- VISITS SUMMARY LOGIC ---
        visits_list = lead.get("visits", [])
        total_duration = 0
        visit_count = len(visits_list)
        last_referrer = "N/A"
        last_session_start = ""
        last_title = ""

        if visits_list:
            # Sum all durations
            total_duration = sum(int(v.get("sessionDuration", 0)) for v in visits_list)
            # Take the FIRST visit in the list as the 'Latest/Primary' one
            latest_visit = visits_list[0]
            last_referrer = latest_visit.get("referrer", "N/A")
            last_session_start = latest_visit.get("sessionStart", "")
            last_title = latest_visit.get("title", "")

        record = {
            "date": report_date, # YYYY-MM-DD
            "id": lead.get("_id"),
            "name": lead.get("name"),
            "phone": lead.get("phone"),
            "website": lead.get("website"),
            "permaScore": float(lead.get("permaScore", 0)),
            "uid": lead.get("uid"),
            
            # Summarized Visit Metrics
            "total_visits_count": visit_count,
            "total_duration_seconds": total_duration,
            "latest_referrer": last_referrer,
            "latest_session_date": last_session_start,
            "latest_page_title": last_title,
            
            # Flattened Address
            "street": address.get("street"),
            "postalCode": address.get("postalCode"),
            "postalCodeInt": int(address.get("postalCodeInt")) if address.get("postalCodeInt") else 0,
            "city": address.get("city"),
            "country": address.get("country"),
            "countryISOCode": address.get("countryISOCode"),
            "countryNumeric": int(address.get("countryNumeric")) if address.get("countryNumeric") else 0,
            
            # Flattened Registration
            "registrationNumber": reg_data.get("registrationNumber", ""),
            "registrationClass": int(reg_data.get("registrationClass", 0)) if reg_data.get("registrationClass") else 0,
            "registrationTypeDescription": reg_data.get("registrationTypeDescription", ""),
            "registrationClassDescription": reg_data.get("registrationClassDescription", "")
        }
        transformed.append(record)
        
    return transformed

def transform_categories(leads_data, report_date):
    print("Transforming CATEGORIES data...")
    transformed = []
    
    for lead in leads_data:
        lead_id = lead.get("_id")
        categories = lead.get("categories", [])
        
        for cat in categories:
            record = {
                "date": report_date, # YYYY-MM-DD
                "id": lead_id, 
                "lead_name": lead.get("name"),
                "category_id": cat.get("id"),
                "category_name": cat.get("name"),
                "category_color": cat.get("hexColor") 
            }
            transformed.append(record)
            
    return transformed

def transform_visits(leads_data, report_date):
    print("Transforming VISITS (Views) data...")
    transformed = []
    
    for lead in leads_data:
        # We can add lead_id here if you want to join datasets in Databox later
        lead_id = lead.get("_id") 
        
        visits = lead.get("visits", [])
        
        for visit in visits:
            # Parent Visit Data
            session_id = visit.get("sessionId")
            session_start = visit.get("sessionStart")
            session_end = visit.get("sessionEnd")
            session_duration = visit.get("sessionDuration")
            user_id = visit.get("userId")
            referrer = visit.get("referrer")
            
            search_list = visit.get("search", [])
            search_str = json.dumps(search_list) if search_list else ""
            
            # Flatten Views
            views = visit.get("views", [])
            
            for view in views:
                record = {
                    "date": report_date, # YYYY-MM-DD
                    
                    # Context
                    "lead_id": lead_id, 
                    "sessionId": session_id,
                    "sessionStart": session_start,
                    "sessionEnd": session_end,
                    "sessionDuration": int(session_duration),
                    "userId": user_id,
                    "search": search_str,
                    "referrer": referrer,
                    
                    # View Specifics
                    "views_time": view.get("time"),
                    "views_pathname": view.get("pathname"),
                    "views_title": view.get("title"),
                    "view_duration": int(view.get("viewDuration", 0)),
                    "title": visit.get("title") 
                }
                transformed.append(record)
                
    return transformed

# --- Databox Push ---

def push_to_databox(data, databox_push_url):
    if not data:
        print("No data to push.")
        return

    print(f"Pushing {len(data)} records to Databox URL: {databox_push_url}...")
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': DATABOX_API_KEY,
        'Accept': 'application/vnd.databox.v1+json'
    }
    
    for i in range(0, len(data), DATABOX_PAYLOAD_LIMIT):
        chunk = data[i:i + DATABOX_PAYLOAD_LIMIT]
        payload = {"records": chunk} 
        
        response = make_api_request("POST", databox_push_url, headers=headers, json_payload=payload)

        if response and (response.status_code == 200 or response.status_code == 202):
            print(f"Chunk {i // DATABOX_PAYLOAD_LIMIT + 1} pushed successfully.")
        else:
            print(f"Push failed for chunk. Status: {response.status_code if response else 'None'}")

# --- Main ---

def main():
    print("Starting Permaleads -> Databox Sync...")
    
    # 1. Authenticate
    token = authenticate_permaleads()
    if not token:
        return

    # # 2. Define Date: Use YESTERDAY (dynamic) in YYYY-MM-DD
    # report_date = get_yesterday_date_str()
    
    # Manual Sync
    report_date = "2025-12-02" 
    
    print(f"Using Report Date: {report_date}")

    # 3. Fetch Data
    leads_data = fetch_daily_report(token, report_date)
    
    if not leads_data:
        print("No leads found for this date. Exiting.")
        return

    print(f"Successfully fetched {len(leads_data)} companies.")
    
    # 4. Transform & Push LEADS
    leads_payload = transform_leads(leads_data, report_date)
    push_to_databox(leads_payload, DATABOX_LEADS_URL)

    # 5. Transform & Push CATEGORIES
    cats_payload = transform_categories(leads_data, report_date)
    push_to_databox(cats_payload, DATABOX_CATS_URL)

    # 6. Transform & Push VISITS
    visits_payload = transform_visits(leads_data, report_date)
    push_to_databox(visits_payload, DATABOX_VISITS_URL)

    print("\nSync Complete.")

if __name__ == "__main__":
    main()