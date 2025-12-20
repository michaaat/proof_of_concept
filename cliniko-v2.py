import requests
from requests.auth import HTTPBasicAuth
import datetime
import pytz
import time
import json
import os
import sys

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# --- 📅 DATE SETTINGS (EDIT HERE FOR HISTORICAL SYNC) ---
# Leave None to run for "Today" (Daily Automation)
# Set dates to run a range: e.g., "2025-11-01" to "2025-11-11"
HISTORICAL_START_DATE = "2025-11-11"  
HISTORICAL_END_DATE = "2025-11-27"   

# --- Cliniko Configuration ---
CLINIKO_API_TOKEN = os.environ.get("CLINIKO_API_TOKEN", "Mxxxx").strip()
CLINIKO_BASE_URL = "https://api.au1.cliniko.com/v1"
CLINIKO_USER_AGENT = "Databox (txxx)"

# --- Databox Configuration ---
DATABOX_API_KEY = os.environ.get("DATABOX_API_KEY", "pak_xxxx").strip()
DATABOX_PAYLOAD_LIMIT = 100 

# Datasets
DATABOX_PATIENT_DATASET_ID = "c9xx"
DATABOX_REFERRAL_DATASET_ID = "dfxx"
DATABOX_BOOKING_DATASET_ID = "4f4xxx"
DATABOX_INVOICE_DATASET_ID = "b40xxx"
DATABOX_INVOICE_ITEM_DATASET_ID = "9ac6a2xxx"
DATABOX_STOCK_ADJUSTMENT_DATASET_ID = "4xx"

# Push URLs
DATABOX_PATIENT_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_PATIENT_DATASET_ID}/data"
DATABOX_REFERRAL_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_REFERRAL_DATASET_ID}/data"
DATABOX_BOOKING_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_BOOKING_DATASET_ID}/data"
DATABOX_INVOICE_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_INVOICE_DATASET_ID}/data"
DATABOX_INVOICE_ITEM_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_INVOICE_ITEM_DATASET_ID}/data"
DATABOX_STOCK_ADJUSTMENT_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_STOCK_ADJUSTMENT_DATASET_ID}/data"

# Verification Templates
DATABOX_PATIENT_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_PATIENT_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_REFERRAL_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_REFERRAL_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_BOOKING_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_BOOKING_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_INVOICE_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_INVOICE_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_INVOICE_ITEM_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_INVOICE_ITEM_DATASET_ID}/ingestions/{{ingestion_id}}"
DATABOX_STOCK_ADJUSTMENT_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_STOCK_ADJUSTMENT_DATASET_ID}/ingestions/{{ingestion_id}}"

# --- Script Settings ---
TIMEZONE = "Australia/Brisbane"
VERIFICATION_WAIT_TIME = 5 
API_MAX_RETRIES = 5
API_INITIAL_BACKOFF = 1 

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

def get_brisbane_time():
    try:
        tz = pytz.timezone(TIMEZONE)
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        return now_utc.astimezone(tz)
    except pytz.UnknownTimeZoneError:
        return datetime.datetime.utcnow()

def get_date_range():
    """
    Generates a list of dates to process.
    1. Env Var MANUAL_DATE (Single Day)
    2. Config HISTORICAL_START/END (Range)
    3. Default (Today)
    """
    env_date = os.environ.get("MANUAL_DATE")
    
    if env_date:
        print(f"📅 Mode: Single Env Date ({env_date})")
        return [datetime.datetime.strptime(env_date, "%Y-%m-%d").date()]
    
    if HISTORICAL_START_DATE and HISTORICAL_END_DATE:
        print(f"📅 Mode: Historical Range ({HISTORICAL_START_DATE} to {HISTORICAL_END_DATE})")
        start = datetime.datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d").date()
        end = datetime.datetime.strptime(HISTORICAL_END_DATE, "%Y-%m-%d").date()
        
        date_list = []
        curr = start
        while curr <= end:
            date_list.append(curr)
            curr += datetime.timedelta(days=1)
        return date_list

    print(f"📅 Mode: Daily Automation (Today)")
    return [get_brisbane_time().date()]

def get_query_config(target_date):
    date_str = target_date.strftime("%Y-%m-%d")
    return {
        "date_str": date_str, 
        "created_from": f"{date_str}T00:00:00Z",
        "created_to": f"{date_str}T23:59:59Z"
    }

def make_api_request(method, url, headers, auth=None, json_payload=None, params=None, max_retries=API_MAX_RETRIES):
    retries = 0
    backoff_time = API_INITIAL_BACKOFF
    while retries < max_retries:
        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, auth=auth, json=json_payload, params=params, timeout=30)
            else:
                response = requests.get(url, headers=headers, auth=auth, params=params, timeout=30)

            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                wait_time = int(response.headers.get("Retry-After", backoff_time)) + 1
                print(f"⚠️ Rate limit (429). Sleeping {wait_time}s...")
                time.sleep(wait_time)
                backoff_time = min(backoff_time * 2, 60)
            elif response.status_code >= 400:
                print(f"⛔ Error {response.status_code}: {response.text[:200]}")
                return response 
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request Exception: {e}. Retrying...")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)
        retries += 1
    return None

def fetch_linked_data(url, headers, auth):
    if not url: return None
    resp = make_api_request("GET", url, headers=headers, auth=auth)
    if resp and resp.status_code == 200:
        return resp.json()
    return None

# ==========================================
# 🧠 PRE-FETCH / CACHING FUNCTIONS
# ==========================================

def prefetch_practitioners(headers, auth):
    print("\n🧠 Pre-fetching Practitioners...")
    mapping = {}
    page = 1
    while True:
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/practitioners", headers=headers, auth=auth, params=[('per_page', '100'), ('page', str(page))])
        if not response or response.status_code != 200: break
        data = response.json()
        for p in data.get('practitioners', []):
            mapping[str(p.get('id'))] = p.get('label', 'N/A')
        if not data.get('links', {}).get('next'): break
        page += 1
    print(f"✅ Cached {len(mapping)} Practitioners.")
    return mapping

def prefetch_businesses(headers, auth):
    print("🧠 Pre-fetching Businesses...")
    mapping = {}
    page = 1
    while True:
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/businesses", headers=headers, auth=auth, params=[('per_page', '100'), ('page', str(page))])
        if not response or response.status_code != 200: break
        data = response.json()
        for b in data.get('businesses', []):
            mapping[str(b.get('id'))] = b.get('business_name', 'N/A')
        if not data.get('links', {}).get('next'): break
        page += 1
    print(f"✅ Cached {len(mapping)} Businesses.")
    return mapping

def prefetch_billable_items(headers, auth):
    print("🧠 Pre-fetching Billable Items...")
    mapping = {}
    page = 1
    while True:
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/billable_items", headers=headers, auth=auth, params=[('per_page', '100'), ('page', str(page))])
        if not response or response.status_code != 200: break
        data = response.json()
        for item in data.get('billable_items', []):
            mapping[str(item.get('id'))] = item.get('name', 'N/A')
        if not data.get('links', {}).get('next'): break
        page += 1
    print(f"✅ Cached {len(mapping)} Billable Items.")
    return mapping

def prefetch_products(headers, auth):
    print("🧠 Pre-fetching Products...")
    mapping = {}
    page = 1
    while True:
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/products", headers=headers, auth=auth, params=[('per_page', '100'), ('page', str(page))])
        if not response or response.status_code != 200: break
        data = response.json()
        for item in data.get('products', []):
            mapping[str(item.get('id'))] = {
                "name": item.get('name', 'N/A'),
                "item_code": item.get('item_code', 'N/A')
            }
        if not data.get('links', {}).get('next'): break
        page += 1
    print(f"✅ Cached {len(mapping)} Products.")
    return mapping

# ==========================================
# 🏥 CLINIKO FETCHERS
# ==========================================

def fetch_cliniko_patients(date_config):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    print(f"   Fetching Patients...")
    while True:
        query_params = [('per_page', '100'), ('page', str(page)), ('q[]', f"created_at:>={date_config['created_from']}"), ('q[]', f"created_at:<={date_config['created_to']}")]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/patients", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break
        try:
            data = response.json()
            all_records.extend(data.get('patients', []))
            if not data.get('links', {}).get('next'): break
            page += 1
        except: break
    return all_records

def fetch_cliniko_referrals(date_config):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    referral_type_cache = {}
    print(f"   Fetching Referrals...")
    while True:
        query_params = [('per_page', '100'), ('page', str(page)), ('q[]', f"created_at:>={date_config['created_from']}"), ('q[]', f"created_at:<={date_config['created_to']}")]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/referral_sources", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break
        try:
            data = response.json()
            raw = data.get('referral_sources', [])
            for ref in raw:
                p_link = ref.get('patient', {}).get('links', {}).get('self')
                p_name = "Unknown"
                if p_link:
                    p_data = fetch_linked_data(p_link, headers, cliniko_auth)
                    if p_data: p_name = p_data.get('label', 'Unknown')
                
                t_link = ref.get('referral_source_type', {}).get('links', {}).get('self')
                t_name = "Unknown"
                if t_link:
                    if t_link in referral_type_cache: t_name = referral_type_cache[t_link]
                    else:
                        t_data = fetch_linked_data(t_link, headers, cliniko_auth)
                        if t_data: 
                            t_name = t_data.get('name', 'Unknown')
                            referral_type_cache[t_link] = t_name
                
                ref['enriched_patient_label'] = p_name
                ref['enriched_type_name'] = t_name
                all_records.append(ref)
            if not data.get('links', {}).get('next'): break
            page += 1
        except: break
    return all_records

def fetch_cliniko_bookings(date_config, practitioner_map, business_map):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    cache_appt_types = {}
    cache_patients = {}
    
    print(f"   Fetching Bookings...")
    while True:
        query_params = [
            ('per_page', '100'), ('page', str(page)), ('sort', 'created_at:desc'),
            ('q[]', f"created_at:>={date_config['created_from']}"),
            ('q[]', f"created_at:<={date_config['created_to']}")
        ]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/bookings", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break
        try:
            data = response.json()
            raw_bookings = data.get('bookings', [])

            for bk in raw_bookings:
                conflict_link = bk.get('conflicts', {}).get('links', {}).get('self')
                bk['conflicts'] = False
                if conflict_link:
                    c_data = fetch_linked_data(conflict_link, headers, cliniko_auth)
                    if c_data and 'conflicts' in c_data:
                        bk['conflicts'] = c_data['conflicts'].get('exist', False)
                
                type_link = bk.get('appointment_type', {}).get('links', {}).get('self')
                bk['appointment_type'] = "N/A"
                if type_link:
                    if type_link in cache_appt_types: bk['appointment_type'] = cache_appt_types[type_link]
                    else:
                        t_data = fetch_linked_data(type_link, headers, cliniko_auth)
                        if t_data:
                            name = t_data.get('name', 'N/A')
                            cache_appt_types[type_link] = name
                            bk['appointment_type'] = name
                
                biz_link = bk.get('business', {}).get('links', {}).get('self')
                bk['business'] = "N/A"
                if biz_link: bk['business'] = business_map.get(biz_link.split('/')[-1], "Unknown")
                
                prac_link = bk.get('practitioner', {}).get('links', {}).get('self')
                bk['practitioner'] = "N/A"
                if prac_link: bk['practitioner'] = practitioner_map.get(prac_link.split('/')[-1], "Unknown")

                pat_link = bk.get('patient', {}).get('links', {}).get('self')
                bk['patient'] = "N/A"
                if pat_link:
                    if pat_link in cache_patients: bk['patient'] = cache_patients[pat_link]
                    else:
                        pat_data = fetch_linked_data(pat_link, headers, cliniko_auth)
                        if pat_data:
                            label = pat_data.get('label', 'N/A')
                            cache_patients[pat_link] = label
                            bk['patient'] = label
                
                bk.pop('attendees', None); bk.pop('links', None)
                all_records.append(bk)
            if not data.get('links', {}).get('next'): break
            page += 1
        except json.JSONDecodeError: break
    return all_records

def fetch_cliniko_invoices(date_config, practitioner_map, business_map):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    cache_patients = {} 
    print(f"   Fetching Invoices...")
    while True:
        query_params = [
            ('per_page', '100'), ('page', str(page)),
            ('q[]', f"created_at:>={date_config['created_from']}"),
            ('q[]', f"created_at:<={date_config['created_to']}")
        ]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/invoices", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break
        try:
            data = response.json()
            raw_invoices = data.get('invoices', [])
            for inv in raw_invoices:
                biz_link = inv.get('business', {}).get('links', {}).get('self')
                inv['business_name'] = "N/A"
                if biz_link: inv['business_name'] = business_map.get(biz_link.split('/')[-1], "Unknown")
                
                prac_link = inv.get('practitioner', {}).get('links', {}).get('self')
                inv['practitioner_name'] = "N/A"
                if prac_link: inv['practitioner_name'] = practitioner_map.get(prac_link.split('/')[-1], "Unknown")

                pat_link = inv.get('patient', {}).get('links', {}).get('self')
                inv['patient_name'] = "N/A"
                if pat_link:
                    if pat_link in cache_patients: inv['patient_name'] = cache_patients[pat_link]
                    else:
                        pat_data = fetch_linked_data(pat_link, headers, cliniko_auth)
                        if pat_data:
                            label = pat_data.get('label', 'N/A')
                            inv['patient_name'] = label
                            cache_patients[pat_link] = label
                
                app_link = inv.get('appointment', {}).get('links', {}).get('self')
                inv['appointment_starts_at'] = None
                if app_link:
                    app_data = fetch_linked_data(app_link, headers, cliniko_auth)
                    if app_data: inv['appointment_starts_at'] = app_data.get('starts_at')
                all_records.append(inv)
            if not data.get('links', {}).get('next'): break
            page += 1
        except json.JSONDecodeError: break
    return all_records

def fetch_cliniko_invoice_items(date_config, billable_item_map):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    cache_invoices = {} 
    print(f"   Fetching Invoice Items...")
    while True:
        query_params = [
            ('per_page', '100'), ('page', str(page)),
            ('q[]', f"created_at:>={date_config['created_from']}"),
            ('q[]', f"created_at:<={date_config['created_to']}")
        ]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/invoice_items", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break

        try:
            data = response.json()
            raw_items = data.get('invoice_items', [])
            for item in raw_items:
                billable_link = item.get('billable_item', {}).get('links', {}).get('self')
                item['billable_item_name'] = "N/A"
                if billable_link:
                    billable_id = billable_link.split('/')[-1]
                    item['billable_item_name'] = billable_item_map.get(billable_id, "Unknown")
                
                invoice_link = item.get('invoice', {}).get('links', {}).get('self')
                item['invoice_number'] = "N/A"
                if invoice_link:
                    if invoice_link in cache_invoices:
                        item['invoice_number'] = cache_invoices[invoice_link]
                    else:
                        inv_data = fetch_linked_data(invoice_link, headers, cliniko_auth)
                        if inv_data:
                            inv_num = str(inv_data.get('number', 'N/A'))
                            item['invoice_number'] = inv_num
                            cache_invoices[invoice_link] = inv_num
                
                all_records.append(item)
            if not data.get('links', {}).get('next'): break
            page += 1
        except json.JSONDecodeError: break
    return all_records

def fetch_cliniko_stock_adjustments(date_config, product_map):
    all_records = []
    page = 1
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    cliniko_auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    print(f"   Fetching Stock Adjustments...")
    while True:
        query_params = [
            ('per_page', '100'), ('page', str(page)),
            ('q[]', f"created_at:>={date_config['created_from']}"),
            ('q[]', f"created_at:<={date_config['created_to']}")
        ]
        response = make_api_request("GET", f"{CLINIKO_BASE_URL}/stock_adjustments", headers=headers, auth=cliniko_auth, params=query_params)
        if not response or response.status_code != 200: break

        try:
            data = response.json()
            raw_adjustments = data.get('stock_adjustments', [])
            for adj in raw_adjustments:
                prod_link = adj.get('product', {}).get('links', {}).get('self')
                adj['product_name'] = "N/A"
                adj['product_item_code'] = "N/A"

                if prod_link:
                    prod_id = prod_link.split('/')[-1]
                    product_data = product_map.get(prod_id, {"name": "Unknown", "item_code": "N/A"})
                    adj['product_name'] = product_data['name']
                    adj['product_item_code'] = product_data['item_code']
                
                all_records.append(adj)

            if not data.get('links', {}).get('next'): break
            page += 1
        except json.JSONDecodeError: break
    return all_records

# ==========================================
# 🔄 TRANSFORMERS
# ==========================================

def transform_patients_data(raw_data, date_str):
    return [{
        "date": date_str, "new_patients": 1, 
        "id": str(p.get('id', '')), "label": str(p.get('label', 'N/A')),
        "city": str(p.get('city', 'N/A')), "state": str(p.get('state', 'N/A')),
        "created_at": str(p.get('created_at', '')), "updated_at": str(p.get('updated_at', ''))
    } for p in raw_data]

def transform_referrals_data(raw_data, date_str):
    return [{
        "date": date_str, "referral_count": 1, 
        "id": str(r.get('id', '')), "notes": str(r.get('notes', '')),
        "patient_name": str(r.get('enriched_patient_label', 'N/A')),
        "referral_type": str(r.get('enriched_type_name', 'N/A')),
        "created_at": str(r.get('created_at', '')), "updated_at": str(r.get('updated_at', ''))
    } for r in raw_data]

def transform_bookings_data(raw_data, date_str):
    return [{
        "date": date_str, "booking_count": 1,
        "id": str(bk.get('id', '')),
        "created_at": str(bk.get('created_at', '')),
        "starts_at": str(bk.get('starts_at', '')),
        "ends_at": str(bk.get('ends_at', '')),
        "notes": str(bk.get('notes', '')),
        "patient_name": str(bk.get('patient', 'N/A')),
        "practitioner_name": str(bk.get('practitioner', 'N/A')),
        "business_name": str(bk.get('business', 'N/A')),
        "appointment_type": str(bk.get('appointment_type', 'N/A')),
        "conflicts": bool(bk.get('conflicts', False)),
        "did_not_arrive": bool(bk.get('did_not_arrive', False)),
        "patient_arrived": bool(bk.get('patient_arrived', False))
    } for bk in raw_data]

def transform_invoices_data(raw_data, date_str):
    transformed = []
    for inv in raw_data:
        total_amount = 0.0
        try: total_amount = float(inv.get('total_amount', 0.0))
        except (ValueError, TypeError): pass 
        transformed.append({
            "date": date_str, "invoice_count": 1, "total_amount": total_amount,
            "id": str(inv.get('id', '')), "number": str(inv.get('number', '')),
            "status": str(inv.get('status_description', 'N/A')),
            "created_at": str(inv.get('created_at', '')),
            "issue_date": str(inv.get('issue_date', '')),
            "closed_at": str(inv.get('closed_at', '')),
            "appointment_starts_at": str(inv.get('appointment_starts_at', '')),
            "patient_name": str(inv.get('patient_name', 'N/A')),
            "practitioner_name": str(inv.get('practitioner_name', 'N/A')),
            "business_name": str(inv.get('business_name', 'N/A'))
        })
    return transformed

def transform_invoice_items_data(raw_data, date_str):
    transformed = []
    for item in raw_data:
        quantity, total = 0.0, 0.0
        try: quantity = float(item.get('quantity', 0.0))
        except (ValueError, TypeError): pass
        try: total = float(item.get('total_including_tax', 0.0))
        except (ValueError, TypeError): pass

        transformed.append({
            "date": date_str,
            "invoice_item_count": 1,
            "quantity": quantity,
            "total_amount": total,
            "id": str(item.get('id', '')),
            "invoice_number": str(item.get('invoice_number', 'N/A')),
            "name": str(item.get('name', 'N/A')),
            "billable_item_name": str(item.get('billable_item_name', 'N/A')),
            "code": str(item.get('code', 'N/A')),
            "created_at": str(item.get('created_at', ''))
        })
    return transformed

def transform_stock_adjustments_data(raw_data, date_str):
    transformed = []
    for adj in raw_data:
        quantity = 0.0
        try: quantity = float(adj.get('quantity', 0.0))
        except (ValueError, TypeError): pass

        transformed.append({
            "date": date_str, "stock_adjustment_count": 1,
            "quantity": quantity,
            "id": str(adj.get('id', '')),
            "adjustment_type": str(adj.get('adjustment_type', 'N/A')),
            "product_name": str(adj.get('product_name', 'N/A')),
            "product_item_code": str(adj.get('product_item_code', 'N/A')),
            "created_at": str(adj.get('created_at', '')),
            "updated_at": str(adj.get('updated_at', ''))
        })
    return transformed

# ==========================================
# 📊 DATABOX PUSH & VERIFY
# ==========================================

def push_to_databox(data, push_url):
    if not data: return []
    print(f"   Pushing {len(data)} records to Databox...")
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'x-api-key': DATABOX_API_KEY}
    ingestion_ids = []
    for i in range(0, len(data), DATABOX_PAYLOAD_LIMIT):
        chunk = data[i:i + DATABOX_PAYLOAD_LIMIT]
        response = make_api_request("POST", push_url, headers=headers, json_payload={"records": chunk})
        if response and response.status_code == 200:
            res_json = response.json()
            i_id = res_json.get("id") or res_json.get("ingestionId")
            if i_id: ingestion_ids.append(i_id)
        else: print("❌ Failed to push chunk.")
    return ingestion_ids

def verify_ingestions(ingestion_ids, url_template):
    if not ingestion_ids: return
    print(f"   Verifying...")
    time.sleep(VERIFICATION_WAIT_TIME)
    headers = {'x-api-key': DATABOX_API_KEY}
    for i_id in ingestion_ids:
        resp = make_api_request("GET", url_template.format(ingestion_id=i_id), headers=headers)
        if resp and resp.status_code == 200:
            status = resp.json().get('status', 'unknown')
            print(f"   🆔 {i_id}: {status.upper()}")

# ==========================================
# 🚀 MAIN
# ==========================================

def main():
    # --- PRE-FETCHING (Once per run) ---
    headers = {'Accept': 'application/json', 'User-Agent': CLINIKO_USER_AGENT}
    auth = HTTPBasicAuth(CLINIKO_API_TOKEN, '')
    
    practitioner_map = prefetch_practitioners(headers, auth)
    business_map = prefetch_businesses(headers, auth)
    billable_item_map = prefetch_billable_items(headers, auth) 
    product_map = prefetch_products(headers, auth) 

    # --- DATE LOOP ---
    date_list = get_date_range()
    
    for target_date in date_list:
        print(f"\n==================================================")
        print(f"🚀 PROCESSING DATE: {target_date}")
        print(f"==================================================")
        
        date_config = get_query_config(target_date)
        
        # 1. PATIENTS
        patients = fetch_cliniko_patients(date_config)
        if patients:
            verify_ingestions(push_to_databox(transform_patients_data(patients, date_config['date_str']), DATABOX_PATIENT_PUSH_URL), DATABOX_PATIENT_VERIFY_URL_TEMPLATE)

        # 2. REFERRALS
        referrals = fetch_cliniko_referrals(date_config)
        if referrals:
            verify_ingestions(push_to_databox(transform_referrals_data(referrals, date_config['date_str']), DATABOX_REFERRAL_PUSH_URL), DATABOX_REFERRAL_VERIFY_URL_TEMPLATE)

        # 3. BOOKINGS
        bookings = fetch_cliniko_bookings(date_config, practitioner_map, business_map)
        if bookings:
            verify_ingestions(push_to_databox(transform_bookings_data(bookings, date_config['date_str']), DATABOX_BOOKING_PUSH_URL), DATABOX_BOOKING_VERIFY_URL_TEMPLATE)

        # 4. INVOICES
        invoices = fetch_cliniko_invoices(date_config, practitioner_map, business_map)
        if invoices:
            verify_ingestions(push_to_databox(transform_invoices_data(invoices, date_config['date_str']), DATABOX_INVOICE_PUSH_URL), DATABOX_INVOICE_VERIFY_URL_TEMPLATE)

        # 5. INVOICE ITEMS
        invoice_items = fetch_cliniko_invoice_items(date_config, billable_item_map)
        if invoice_items:
            verify_ingestions(push_to_databox(transform_invoice_items_data(invoice_items, date_config['date_str']), DATABOX_INVOICE_ITEM_PUSH_URL), DATABOX_INVOICE_ITEM_VERIFY_URL_TEMPLATE)

        # 6. STOCK ADJUSTMENTS
        stock_adjustments = fetch_cliniko_stock_adjustments(date_config, product_map)
        if stock_adjustments:
            verify_ingestions(push_to_databox(transform_stock_adjustments_data(stock_adjustments, date_config['date_str']), DATABOX_STOCK_ADJUSTMENT_PUSH_URL), DATABOX_STOCK_ADJUSTMENT_VERIFY_URL_TEMPLATE)

    print("\n✅ ALL PROCESSING COMPLETE.")

if __name__ == "__main__":
    main()