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

# --- 📅 DATE SETTINGS ---
# Set to string dates (YYYY-MM-DD) for historical range.
# Set to None for "Daily Mode" (Today).
HISTORICAL_START_DATE = ""
HISTORICAL_END_DATE = ""

# --- ShipStation Configuration ---
SHIPSTATION_API_KEY = "x"
SHIPSTATION_API_SECRET = "xx"
SHIPSTATION_BASE_URL = "https://ssapi.shipstation.com"

# --- Databox Configuration ---
DATABOX_API_TOKEN = "pak_xx"
DATABOX_PAYLOAD_LIMIT = 100

# Datasets
DATABOX_ORDERS_DATASET_ID = "cf0bae06xxx"
DATABOX_ORDER_ITEMS_DATASET_ID = "c23xx"
DATABOX_SHIPMENT_DATASET_ID = "7153xx" 

# Push & Verify URLs
# 1. Orders
DATABOX_ORDERS_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_ORDERS_DATASET_ID}/data"
DATABOX_ORDERS_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_ORDERS_DATASET_ID}/ingestions/{{ingestion_id}}"

# 2. Order Line Items
DATABOX_ORDER_ITEMS_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_ORDER_ITEMS_DATASET_ID}/data"
DATABOX_ORDER_ITEMS_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_ORDER_ITEMS_DATASET_ID}/ingestions/{{ingestion_id}}"

# 3. Shipments
DATABOX_SHIPMENTS_PUSH_URL = f"https://api.databox.com/v1/datasets/{DATABOX_SHIPMENT_DATASET_ID}/data"
DATABOX_SHIPMENTS_VERIFY_URL_TEMPLATE = f"https://api.databox.com/v1/datasets/{DATABOX_SHIPMENT_DATASET_ID}/ingestions/{{ingestion_id}}"

# --- Script Settings ---
TIMEZONE = "America/Chicago"
API_MAX_RETRIES = 5
API_INITIAL_BACKOFF = 1
RATE_LIMIT_BUFFER = 2 
VERIFICATION_WAIT_TIME = 3

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

def get_chicago_time():
    try:
        tz = pytz.timezone(TIMEZONE)
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        return now_utc.astimezone(tz)
    except pytz.UnknownTimeZoneError:
        return datetime.datetime.utcnow()

def get_date_range():
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
    today_chicago = get_chicago_time().date()
    print(f"📅 Mode: Daily Automation (Today in Chicago: {today_chicago})")
    return [today_chicago]

def handle_rate_limits(response):
    """Checks rate limits for both ShipStation and Databox headers."""
    try:
        if "X-Rate-Limit-Reset" in response.headers:
            remaining = int(response.headers.get("X-Rate-Limit-Remaining", 100))
            reset_seconds = int(response.headers.get("X-Rate-Limit-Reset", 0))
            if remaining < 2:
                sleep_time = reset_seconds + RATE_LIMIT_BUFFER
                print(f"⏳ ShipStation Rate limit approaching ({remaining} left). Sleeping for {sleep_time}s...")
                time.sleep(sleep_time)
        elif "X-RateLimit-Remaining" in response.headers:
             remaining = int(response.headers.get("X-RateLimit-Remaining", 1000))
             if remaining < 5:
                 print(f"⏳ Databox Rate limit approaching ({remaining} left). Pausing briefly...")
                 time.sleep(2)
    except (ValueError, TypeError):
        pass

def make_api_request(method, url, headers=None, auth=None, json_payload=None, params=None, max_retries=API_MAX_RETRIES):
    retries = 0
    backoff_time = API_INITIAL_BACKOFF
    
    while retries < max_retries:
        try:
            if method.upper() == "POST":
                response = requests.post(url, headers=headers, auth=auth, json=json_payload, params=params, timeout=30)
            else:
                response = requests.get(url, headers=headers, auth=auth, params=params, timeout=30)

            handle_rate_limits(response)

            if response.status_code == 200:
                return response
            elif response.status_code == 429:
                retry_after = 60
                if response.headers.get("Retry-After"):
                    try: retry_after = int(response.headers.get("Retry-After"))
                    except: pass
                elif response.headers.get("X-Rate-Limit-Reset"):
                    try: retry_after = int(response.headers.get("X-Rate-Limit-Reset"))
                    except: pass
                
                wait_time = retry_after + RATE_LIMIT_BUFFER
                print(f"⚠️ 429 Too Many Requests. Sleeping {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif response.status_code >= 400:
                print(f"⛔ Error {response.status_code}: {response.text[:200]}")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request Exception: {e}. Retrying...")
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, 60)
        retries += 1
    return None

# ==========================================
# 📦 SHIPSTATION FETCHERS
# ==========================================

def fetch_shipstation_orders(target_date):
    """
    Fetches orders CREATED on the target_date.
    """
    all_orders = []
    page = 1
    page_size = 500
    date_str = target_date.strftime("%Y-%m-%d")
    auth = HTTPBasicAuth(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)
    headers = {'Content-Type': 'application/json'}

    print(f"  Fetching Orders (Created) for {date_str}...")

    while True:
        params = {
            "createDateStart": f"{date_str}T00:00:00.0000000",
            "createDateEnd": f"{date_str}T23:59:59.9999999",
            "pageSize": page_size,
            "page": page
        }
        response = make_api_request("GET", f"{SHIPSTATION_BASE_URL}/orders", headers=headers, auth=auth, params=params)
        if not response or response.status_code != 200: break
        data = response.json()
        orders = data.get("orders", [])
        if not orders: break
        all_orders.extend(orders)
        
        total = data.get("total", 0)
        pages = data.get("pages", 0)
        if page >= pages: break
        page += 1
        
    print(f"  ✅ Found {len(all_orders)} created orders.")
    return all_orders

def fetch_modified_orders(target_date):
    """
    Fetches orders MODIFIED within a window starting from target_date.
    Uses a multi-day window (default 3 days) to capture updates effectively.
    """
    all_orders = []
    page = 1
    page_size = 500
    
    # Calculate start and end date for the modification window
    start_date_str = target_date.strftime("%Y-%m-%d")
    # Extend end date by 2 days to make it a 3-day window [target, target+1, target+2]
    # Adjust delta days as needed
    end_date_obj = target_date + datetime.timedelta(days=2) 
    end_date_str = end_date_obj.strftime("%Y-%m-%d")
    
    auth = HTTPBasicAuth(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)
    headers = {'Content-Type': 'application/json'}

    print(f"  Fetching Orders (Modified) from {start_date_str} to {end_date_str}...")

    while True:
        params = {
            "modifyDateStart": f"{start_date_str}T00:00:00.0000000",
            "modifyDateEnd": f"{end_date_str}T23:59:59.9999999",
            "pageSize": page_size,
            "page": page
        }
        response = make_api_request("GET", f"{SHIPSTATION_BASE_URL}/orders", headers=headers, auth=auth, params=params)
        if not response or response.status_code != 200: break
        data = response.json()
        orders = data.get("orders", [])
        if not orders: break
        all_orders.extend(orders)
        
        total = data.get("total", 0)
        pages = data.get("pages", 0)
        if page >= pages: break
        page += 1
        
    print(f"  ✅ Found {len(all_orders)} modified orders.")
    return all_orders

def fetch_shipstation_shipments(target_date):
    all_shipments = []
    page = 1
    page_size = 500
    date_str = target_date.strftime("%Y-%m-%d")
    auth = HTTPBasicAuth(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET)
    headers = {'Content-Type': 'application/json'}

    print(f"  Fetching Shipments for {date_str}...")

    while True:
        params = {
            "createDateStart": f"{date_str}T00:00:00.0000000",
            "createDateEnd": f"{date_str}T23:59:59.9999999",
            "pageSize": page_size,
            "page": page
        }
        response = make_api_request("GET", f"{SHIPSTATION_BASE_URL}/shipments", headers=headers, auth=auth, params=params)
        if not response or response.status_code != 200: break
        data = response.json()
        shipments = data.get("shipments", [])
        if not shipments: break
        all_shipments.extend(shipments)
        
        total = data.get("total", 0)
        pages = data.get("pages", 0)
        if page >= pages: break
        page += 1
        
    print(f"  ✅ Found {len(all_shipments)} shipments.")
    return all_shipments

# ==========================================
# 🔄 TRANSFORMERS
# ==========================================

def clean_text(text):
    """Helper to clean text fields: remove newlines, strip whitespace, handle None."""
    if text is None:
        return ""
    return str(text).replace('\n', ' ').replace('\r', ' ').strip()

def flatten_obj(obj, prefix):
    """Generic flattening with null safety."""
    flat = {}
    if not obj: return flat
    for k, v in obj.items():
        if isinstance(v, str):
            flat[f"{prefix}_{k}"] = clean_text(v)
        else:
            flat[f"{prefix}_{k}"] = v
    return flat

def get_sku_or_fallback(sku, item_id):
    """
    Returns SKU if valid, otherwise generates a unique fallback using orderItemId.
    This satisfies Databox's non-null key requirement.
    """
    if sku and str(sku).strip():
        return str(sku).strip()
    return f"NO_SKU_{item_id}"

def transform_orders(orders):
    records = []
    for o in orders:
        base_record = o.copy()
        if "items" in base_record:
            del base_record["items"]
            
        if "giftMessage" in base_record: base_record["giftMessage"] = clean_text(base_record["giftMessage"])
        if "internalNotes" in base_record: base_record["internalNotes"] = clean_text(base_record["internalNotes"])
        if "customerNotes" in base_record: base_record["customerNotes"] = clean_text(base_record["customerNotes"])
        
        if "billTo" in base_record: base_record.update(flatten_obj(base_record.pop("billTo"), "billTo"))
        if "shipTo" in base_record: base_record.update(flatten_obj(base_record.pop("shipTo"), "shipTo"))
        if "weight" in base_record: base_record.update(flatten_obj(base_record.pop("weight"), "weight"))
        if "dimensions" in base_record: base_record.update(flatten_obj(base_record.pop("dimensions"), "dimensions"))
        if "insuranceOptions" in base_record: base_record.update(flatten_obj(base_record.pop("insuranceOptions"), "insuranceOptions"))
        if "advancedOptions" in base_record: base_record.update(flatten_obj(base_record.pop("advancedOptions"), "advancedOptions"))

        if "tagIds" in base_record and isinstance(base_record["tagIds"], list):
            base_record["tagIds"] = ", ".join(map(str, base_record["tagIds"]))
            
        records.append(base_record)
    return records

def transform_order_items(orders):
    records = []
    for o in orders:
        raw_items = o.get("items", [])
        if not raw_items: continue
        
        order_id = o.get("orderId")
        order_number = o.get("orderNumber")
        order_date = o.get("orderDate")
        create_date = o.get("createDate")
        
        for item in raw_items:
            item_record = {}
            
            item_record["orderId"] = order_id
            item_record["orderNumber"] = order_number
            item_record["orderDate"] = order_date
            item_record["createDate"] = create_date
            
            item_id = item.get("orderItemId")
            item_record["orderItemId"] = item_id
            
            # 🛑 FIX: Handle Null SKU for Databox Primary Key
            raw_sku = item.get("sku")
            item_record["sku"] = get_sku_or_fallback(raw_sku, item_id)
            
            item_record["lineItemKey"] = item.get("lineItemKey")
            item_record["name"] = clean_text(item.get("name"))
            item_record["quantity"] = item.get("quantity")
            item_record["unitPrice"] = item.get("unitPrice")
            item_record["warehouseLocation"] = item.get("warehouseLocation")
            item_record["productId"] = item.get("productId")
            item_record["fulfillmentSku"] = item.get("fulfillmentSku")
            item_record["adjustment"] = item.get("adjustment")
            item_record["upc"] = item.get("upc")
            
            if "weight" in item:
                item_record.update(flatten_obj(item.get("weight"), "weight"))
            else:
                item_record["weight_value"] = None
                item_record["weight_units"] = None
                item_record["weight_WeightUnits"] = None
            
            records.append(item_record)
    return records

def transform_shipments(shipments):
    records = []
    for s in shipments:
        record = s.copy()
        if "shipTo" in record: record.update(flatten_obj(record.pop("shipTo"), "shipTo"))
        if "weight" in record: record.update(flatten_obj(record.pop("weight"), "weight"))
        if "dimensions" in record: record.update(flatten_obj(record.pop("dimensions"), "dimensions"))
        if "insuranceOptions" in record: record.update(flatten_obj(record.pop("insuranceOptions"), "insuranceOptions"))
        if "advancedOptions" in record: record.update(flatten_obj(record.pop("advancedOptions"), "advancedOptions"))
        records.append(record)
    return records

# ==========================================
# 📊 DATABOX PUSH & VERIFY
# ==========================================

def push_to_databox(data, push_url):
    if not data: return []
    print(f"  Pushing {len(data)} records to Databox...")
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'x-api-key': DATABOX_API_TOKEN}
    ingestion_ids = []
    for i in range(0, len(data), DATABOX_PAYLOAD_LIMIT):
        chunk = data[i:i + DATABOX_PAYLOAD_LIMIT]
        payload = {"records": chunk} 
        response = make_api_request("POST", push_url, headers=headers, json_payload=payload)
        if response and response.status_code == 200:
            res_json = response.json()
            i_id = res_json.get("id") or res_json.get("ingestionId")
            if i_id: ingestion_ids.append(i_id)
            print(f"  ✅ Chunk {i // DATABOX_PAYLOAD_LIMIT + 1} pushed.")
        else:
            print(f"  ❌ Chunk {i // DATABOX_PAYLOAD_LIMIT + 1} failed.")
    return ingestion_ids

def verify_ingestions(ingestion_ids, url_template):
    if not ingestion_ids: return
    print(f"  Verifying {len(ingestion_ids)} chunks...")
    time.sleep(VERIFICATION_WAIT_TIME)
    headers = {'Accept': 'application/json', 'x-api-key': DATABOX_API_TOKEN}
    for i_id in ingestion_ids:
        url = url_template.format(ingestion_id=i_id)
        resp = make_api_request("GET", url, headers=headers)
        if resp and resp.status_code == 200:
            data = resp.json()
            status = data.get('status', 'unknown')
            icon = "✅" if status.lower() in ["processed", "ok"] else "⚠️"
            print(f"  🆔 {i_id}: {icon} {status.upper()}")
            if data.get('errors'): print(f"     ❌ Errors: {data.get('errors')}")

# ==========================================
# 🚀 MAIN
# ==========================================

def main():
    date_list = get_date_range()
    
    for target_date in date_list:
        print(f"\n==================================================")
        print(f"🚀 PROCESSING DATE: {target_date}")
        print(f"==================================================")
        
        # --- 1. NEW ORDERS (Fetch Created) ---
        orders_raw = fetch_shipstation_orders(target_date)
        if orders_raw:
            orders_transformed = transform_orders(orders_raw)
            print(f"👉 Processing Created Orders Dataset ({len(orders_transformed)} records)...")
            ingestion_ids = push_to_databox(orders_transformed, DATABOX_ORDERS_PUSH_URL)
            verify_ingestions(ingestion_ids, DATABOX_ORDERS_VERIFY_URL_TEMPLATE)
            
            items_transformed = transform_order_items(orders_raw)
            print(f"👉 Processing Created Order Items Dataset ({len(items_transformed)} records)...")
            ingestion_ids_items = push_to_databox(items_transformed, DATABOX_ORDER_ITEMS_PUSH_URL)
            verify_ingestions(ingestion_ids_items, DATABOX_ORDER_ITEMS_VERIFY_URL_TEMPLATE)
            
        # --- 2. MODIFIED ORDERS (Fetch Updates) ---
        # Fetching modifications in a 3-day window starting from target_date
        modified_orders = fetch_modified_orders(target_date)
        if modified_orders:
            # We reuse the same transformation logic because the data structure is identical.
            # Databox will update existing records based on the unique 'orderId'.
            modified_transformed = transform_orders(modified_orders)
            print(f"👉 Processing Modified Orders Dataset ({len(modified_transformed)} records)...")
            ingestion_ids_mod = push_to_databox(modified_transformed, DATABOX_ORDERS_PUSH_URL)
            verify_ingestions(ingestion_ids_mod, DATABOX_ORDERS_VERIFY_URL_TEMPLATE)
            
            # Note: We technically could also update Order Items here if items changed, 
            # but usually order-level modifications (status, notes) are the priority.
            # If item details change frequently, you could uncomment the lines below:
            # modified_items = transform_order_items(modified_orders)
            # push_to_databox(modified_items, DATABOX_ORDER_ITEMS_PUSH_URL)
            
        # --- 3. SHIPMENTS ---
        shipments_raw = fetch_shipstation_shipments(target_date)
        if shipments_raw:
            shipments_transformed = transform_shipments(shipments_raw)
            print(f"👉 Processing Shipments Dataset ({len(shipments_transformed)} records)...")
            ingestion_ids = push_to_databox(shipments_transformed, DATABOX_SHIPMENTS_PUSH_URL)
            verify_ingestions(ingestion_ids, DATABOX_SHIPMENTS_VERIFY_URL_TEMPLATE)
            
    print("\n✅ ALL PROCESSING COMPLETE.")

if __name__ == "__main__":
    main()