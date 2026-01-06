import requests
import datetime
import pytz
import time
import json

# --- Configuration ---

# Dealer Credentials Configuration
DEALERS = [
    {
        "name": "Benzel Busch",
        "clientId": "31xxxxxx",
        "apiKey": "2dxxxxx"
    },
    {
        "name": "Genesis of Englewood",
        "clientId": "3cxxx",
        "apiKey": "6bxx"
    },
    {
        "name": "Mercedes-Benz of Orange County",
        "clientId": "faxxx",
        "apiKey": "160xx"
    }
]

MOMENTUM_AUTH_BASE_URL = "https://momapi.udccrm.com:8443"
MOMENTUM_APP_TYPE = "V"

# --- Databox Configuration ---
DATABOX_API_KEY = "pak_353xxxx"
DATABOX_PAYLOAD_LIMIT = 100 

# Dataset 1: Sales (Deals + Leads)
DATABOX_SALES_DATASET_ID = "49xxx"
DATABOX_SALES_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_SALES_DATASET_ID}/data"
DATABOX_SALES_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_SALES_DATASET_ID}/ingestions/{{ingestion_id}}"

# Dataset 2: Opportunities (Prospects)
DATABOX_OPPS_DATASET_ID = "4xx"
DATABOX_OPPS_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_OPPS_DATASET_ID}/data"
DATABOX_OPPS_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_OPPS_DATASET_ID}/ingestions/{{ingestion_id}}"


# --- Script Configuration ---
TIMEZONE = "America/New_York" 
VERIFICATION_WAIT_TIME = 10  # Seconds to wait before checking ingestion status
API_MAX_RETRIES = 5
API_INITIAL_BACKOFF = 1  # Seconds

# --- Helper Functions ---

def get_nested(data, keys, default=''):
    """
    Safely retrieves a nested value from a dictionary.
    """
    temp = data
    for key in keys:
        if isinstance(temp, dict):
            temp = temp.get(key)
        else:
            return default
    return temp if temp is not None else default

def get_newyork_time():
    try:
        tz = pytz.timezone(TIMEZONE)
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        now_newyork = now_utc.astimezone(tz)
        return now_newyork
    except pytz.UnknownTimeZoneError:
        print(f"Error: Unknown timezone '{TIMEZONE}'. Defaulting to UTC.")
        return datetime.datetime.utcnow()

def get_dynamic_dates():
    now = get_newyork_time()
    
    # Calculate 'updatedSince'
    three_days_ago = now - datetime.timedelta(days=3)
    
    updated_since_str = three_days_ago.strftime("%Y-%m-%d")

    #  # Calculate 6 months (approx 183 days) ago for 'updatedSince'
    # six_months_ago = now - datetime.timedelta(days=183)
    
    # updated_since_str = six_months_ago.strftime("%Y-%m-%d")
    
    # Static dates as per your example
    deal_date_from = "2025-11-20"
    deal_date_to = "2025-12-04"

    created_from = "2025-11-20"
    created_to = "2025-12-24"

    print(f"Using dates: updatedSince={updated_since_str}, dealDateFrom={deal_date_from}, dealDateTo={deal_date_to}")

    return {
        "updatedSince": updated_since_str,
        "dealDateFrom": deal_date_from,
        "dealDateTo": deal_date_to,
        "createdFrom": created_from,
        "createdTo": created_to
    }

def make_api_request(method, url, headers, json_payload=None, params=None, max_retries=API_MAX_RETRIES):
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
                wait_time_str = response.headers.get("Retry-After")
                wait_time = int(wait_time_str) if wait_time_str else backoff_time
                print(f"Rate limit hit. Waiting {wait_time}s")
                time.sleep(wait_time)
                backoff_time = min(backoff_time * 2, 60)

            elif response.status_code >= 500:
                print(f"Server error ({response.status_code}). Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
            
            else:
                print(f"Client error: {response.status_code}. URL: {url}")
                return response 

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}. Retrying in {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)
        
        retries += 1

    print(f"Max retries ({max_retries}) exceeded for request to {url}.")
    return None

# --- Authentication Function ---

def authenticate_momentum(client_id, api_key):
    """
    Authenticates with Momentum API to retrieve a dynamic Session Token and Base URL.
    """
    url = f"{MOMENTUM_AUTH_BASE_URL}/{client_id}"
    
    headers = {
        'MOM-ApplicationType': MOMENTUM_APP_TYPE,
        'MOM-Api-Key': api_key
    }
    
    print(f"Authenticating for Client ID: {client_id}...")
    response = make_api_request("GET", url, headers=headers)
    
    if response and response.status_code == 200:
        try:
            data = response.json()
            api_token = data.get("apiToken")
            end_point = data.get("endPoint")
            
            if api_token and end_point:
                print("Authentication successful.")
                return api_token, end_point
            else:
                print("Authentication failed: Missing apiToken or endPoint in response.")
                return None, None
        except json.JSONDecodeError:
            print("Authentication failed: Could not decode JSON.")
            return None, None
    else:
        print("Authentication request failed.")
        return None, None

def fetch_all_momentum_data(base_url, base_params, api_token):
    """
    Fetches all data from a Momentum endpoint, handling pagination.
    """
    all_records = []
    current_params = base_params.copy()
    
    momentum_headers = {
        'Content-Type': 'application/json',
        'MOM-ApplicationType': MOMENTUM_APP_TYPE,
        'MOM-Api-Token': api_token 
    }
    
    page_count = 1
    
    while True:
        print(f"Fetching page {page_count} from {base_url}...")
        response = make_api_request("GET", base_url, headers=momentum_headers, params=current_params)
        
        if response is None or response.status_code != 200:
            print(f"Error fetching data from {base_url}. Halting this data pull.")
            break
            
        try:
            data = response.json()
            records = data.get('data', [])
            all_records.extend(records)
            print(f"Fetched {len(records)} records. Total: {len(all_records)}")
            
            next_fetch_key = data.get('meta', {}).get('nextFetchKey')
            
            if next_fetch_key:
                current_params['fetchKey'] = next_fetch_key
                page_count += 1
            else:
                print("No nextFetchKey found. Pagination complete.")
                break
                
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON response.")
            break
            
    return all_records

# --- WORKFLOW 1: SALES (Deals + Leads) ---

def transform_data_sales(deals_data, leads_data):
    print("Starting SALES data transformation...")
    
    print(f"Building leads lookup map from {len(leads_data)} leads...")
    leads_map = {}
    for lead in leads_data:
        prospect_id = lead.get('prospectApiID')
        if prospect_id:
            leads_map[prospect_id] = {
                'leadSource': lead.get('leadSource', 'N/A'),
                'leadSourceCategory': lead.get('leadSourceCategory', 'N/A')
            }
    
    transformed_data = []
    date_str = get_newyork_time().strftime("%Y-%m-%d")
    
    for deal_record in deals_data:
        prospect_id = deal_record.get('prospectApiID')
        lead_info = leads_map.get(prospect_id, {})

        make = get_nested(deal_record, ['purchaseVehicle', 'make'], '')
        model = get_nested(deal_record, ['purchaseVehicle', 'model'], '')
        make_model = f"{make} {model}".strip()

        record = {
            'person_api_id': get_nested(deal_record, ['personApiID']),
            'dm_customer_number': get_nested(deal_record, ['dmsCustomerNumber']),
            'date': get_nested(deal_record, ['deal', 'dealDate'], date_str),
            'dealer': str(get_nested(deal_record, ['dealer', 'name'], 'N/A')),
            'prospect_id': str(prospect_id if prospect_id else 'N/A'),
            'vehicle_condition': str(get_nested(deal_record, ['purchaseVehicle', 'newUsed'], 'N/A')),
            'vin': str(get_nested(deal_record, ['purchaseVehicle', 'vin'], 'N/A')),
            'year': str(get_nested(deal_record, ['purchaseVehicle', 'year'], '')),
            'make_model': str(make_model),
            'sales_amount': str(get_nested(deal_record, ['deal', 'sellingPrice'], 0.0)),
            'lead_source': str(lead_info.get('leadSource', 'N/A')),
            'lead_category': str(lead_info.get('leadSourceCategory', 'N/A'))
        }
        
        transformed_data.append(record)
        
    print(f"SALES transformation complete. {len(transformed_data)} records prepared.")
    return transformed_data

def sync_sales_dataset(dates, api_token, base_url, dealer_name):
    print(f"\n--- STARTING SALES SYNC: {dealer_name} ---")
    
    deals_endpoint = f"{base_url}/CDP/deal"
    leads_endpoint = f"{base_url}/CDP/lead"

    deal_params = {
        'dealDateFrom': dates['dealDateFrom'],
        'dealDateTo': dates['dealDateTo'],
        'updatedSince': dates['updatedSince'],
        'limit': 2500
    }
    
    lead_params = {
        'createdFrom': dates['createdFrom'],
        'createdTo': dates['createdTo'],
        'updatedSince': dates['updatedSince'],
        'limit': 2500
    }
    
    print("--- Fetching Deals ---")
    deals_data = fetch_all_momentum_data(deals_endpoint, deal_params, api_token)
    if not deals_data:
        print(f"No deals data found for {dealer_name}. Skipping Sales sync.")
        return

    print("\n--- Fetching Leads ---")
    leads_data = fetch_all_momentum_data(leads_endpoint, lead_params, api_token)
    if not leads_data:
        print(f"No leads data found for {dealer_name}. Proceeding with deals only.")
    
    transformed_data = transform_data_sales(deals_data, leads_data)
    
    if transformed_data:
        ingestion_ids = push_to_databox(transformed_data, DATABOX_SALES_PUSH_URL)
        verify_databox_ingestion(ingestion_ids, DATABOX_SALES_VERIFY_URL_TEMPLATE)
    else:
        print("No data transformed.")

# --- WORKFLOW 2: OPPORTUNITIES (Prospects) ---

def transform_data_opportunities(prospect_data):
    print("Starting OPPORTUNITIES data transformation...")
    
    transformed_data = []
    
    for record in prospect_data:
        date_str = get_nested(record, ['creationDate'], '')
        if 'T' in date_str:
            date_str = date_str.split('T')[0]
        else:
             date_str = get_newyork_time().strftime("%Y-%m-%d")

        transformed_record = {
            'date': date_str,
            'createdByEmployeeApiID': str(get_nested(record, ['createdByEmployeeApiID'])),
            'creationDate': str(get_nested(record, ['creationDate'])),
            'updateDate': str(get_nested(record, ['updateDate'])),
            'closedByDate': str(get_nested(record, ['closedByDate'])),
            'closedReason': str(get_nested(record, ['closedReason'])),
            'make': str(get_nested(record, ['voi', 'make'])),
            'year': str(get_nested(record, ['voi', 'year'])),
            'bdcStatus': str(get_nested(record, ['bdcStatus', 'status'])),
            'salesStatus': str(get_nested(record, ['salesStatus', 'status'])),
            'dealer': str(get_nested(record, ['dealer', 'name'])),
            'prospect_id': str(get_nested(record, ['prospectApiID'])),
            'personApiID': str(get_nested(record, ['personApiID'])),
            'leadType': str(get_nested(record, ['leadType'])),
            'leadSource': str(get_nested(record, ['leadSource'])),
            'leadSourceCategory': str(get_nested(record, ['leadSourceCategory']))
        }
        
        transformed_data.append(transformed_record)
        
    print(f"OPPORTUNITIES transformation complete. {len(transformed_data)} records prepared.")
    return transformed_data

def sync_opportunities_dataset(dates, api_token, base_url, dealer_name):
    print(f"\n--- STARTING OPPORTUNITIES SYNC: {dealer_name} ---")
    
    prospect_endpoint = f"{base_url}/CDP/prospect"

    prospect_params = {
        'createdFrom': dates['createdFrom'],
        'createdTo': dates['createdTo'],
        'updatedSince': dates['updatedSince'],
        'limit': 2500 
    }
    
    print("--- Fetching Prospects ---")
    prospect_data = fetch_all_momentum_data(prospect_endpoint, prospect_params, api_token)
    
    if not prospect_data:
        print(f"No prospect data found for {dealer_name}. Skipping Opportunities sync.")
        return
    
    transformed_data = transform_data_opportunities(prospect_data)
    
    if transformed_data:
        ingestion_ids = push_to_databox(transformed_data, DATABOX_OPPS_PUSH_URL)
        verify_databox_ingestion(ingestion_ids, DATABOX_OPPS_VERIFY_URL_TEMPLATE)
    else:
        print("No data transformed.")

# --- GENERIC DATABOX FUNCTIONS ---

def push_to_databox(data, databox_push_url):
    print(f"Pushing {len(data)} records to Databox...")
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': DATABOX_API_KEY
    }
    
    ingestion_ids = []
    for i in range(0, len(data), DATABOX_PAYLOAD_LIMIT):
        chunk = data[i:i + DATABOX_PAYLOAD_LIMIT]
        payload = {"records": chunk}
        response = make_api_request("POST", databox_push_url, headers=headers, json_payload=payload)

        if response and response.status_code == 200:
            try:
                result = response.json()
                if result.get("message") == "Data ingestion request accepted":
                    ingestion_ids.append(result["ingestionId"])
                    print(f"Chunk pushed. ID: {result['ingestionId']}")
            except:
                pass
        else:
             print(f"Push failed for chunk. Status: {response.status_code if response else 'None'}")

    return ingestion_ids

def verify_databox_ingestion(ingestion_ids, verify_url_template):
    if not ingestion_ids: return

    print(f"Verifying {len(ingestion_ids)} ingestions in {VERIFICATION_WAIT_TIME}s...")
    time.sleep(VERIFICATION_WAIT_TIME)
    
    headers = {'x-api-key': DATABOX_API_KEY}
    
    for ingestion_id in ingestion_ids:
        verify_url = verify_url_template.format(ingestion_id=ingestion_id)
        response = make_api_request("GET", verify_url, headers=headers)
        if response and response.status_code == 200:
            status = response.json().get("status")
            print(f"Ingestion {ingestion_id}: {status}")

# --- MAIN ORCHESTRATOR ---

def main():
    print("Starting Momentum -> Databox Multi-Dealer Sync...")
    
    dates = get_dynamic_dates()
    
    for dealer in DEALERS:
        print("\n" + "#"*50)
        print(f"PROCESSING DEALER: {dealer['name']}")
        print("#"*50)
        
        # 1. Authenticate Dealer
        api_token, endpoint = authenticate_momentum(dealer['clientId'], dealer['apiKey'])
        
        if api_token and endpoint:
            # Construct versioned base URL (e.g. .../api/v2.1)
            base_url = f"{endpoint}/v2.1"
            
            # 2. Run Sales Sync
            try:
                sync_sales_dataset(dates, api_token, base_url, dealer['name'])
            except Exception as e:
                print(f"ERROR in Sales Sync for {dealer['name']}: {e}")

            # 3. Run Opportunities Sync
            try:
                sync_opportunities_dataset(dates, api_token, base_url, dealer['name'])
            except Exception as e:
                print(f"ERROR in Opportunities Sync for {dealer['name']}: {e}")
        else:
            print(f"Skipping {dealer['name']} due to authentication failure.")

    print("\nAll dealer processes finished.")

if __name__ == "__main__":
    main()