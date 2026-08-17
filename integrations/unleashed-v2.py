"""
Unleashed Software → Databox ETL (DAILY INCREMENTAL sync)

Same three-phase pipeline as the backfill script, but only pulls Sales Orders
that have been created or modified in the last N days (default: 7).

Use this for scheduled daily runs. Use the *_fast.py script for one-off backfills.

Behavior:
  - Computes `since = now_utc - LOOKBACK_DAYS`
  - Calls /SalesOrders?modifiedSince=<since>&pageSize=200
  - Pushes to the same two Databox datasets
  - Upserts by Guid (idempotent — safe to re-run)

Authentication: HMAC-SHA256 request signing.
"""

import requests
import hmac
import hashlib
import base64
import re
import time
import os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# --- 🔑 Unleashed Credentials ---
UNLEASHED_API_ID = "c8dfbe28-xxxxxx"
UNLEASHED_API_KEY = "m+5rlI3mHltmApxxxxx"
UNLEASHED_HOST = "https://api.unleashedsoftware.com"
CLIENT_TYPE = "xxxx"

# --- 📦 Databox Credentials ---
DATABOX_TOKEN = "pak_xxxxxx"
SO_HEADER_DATASET_ID = "8ac965cf-xxxxxx"
SO_LINE_DATASET_ID = "6bd7fed1-xxxxx"

DATABOX_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "x-api-key": DATABOX_TOKEN,
}

# --- ⚙️ Sync Settings ---
# How many days to look back. 7 = safe default for daily runs.
# Override at runtime by setting the LOOKBACK_DAYS env var (handy for GitHub Actions).
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))

PAGE_SIZE = 200
BATCH_SIZE = 100
FETCH_CONCURRENCY = 3
PUSH_CONCURRENCY = 3
MAX_PAGES = None  # None = all matching pages

# ==========================================
# 🛠️ HELPERS
# ==========================================

def sign(query_string: str) -> str:
    digest = hmac.new(
        UNLEASHED_API_KEY.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def unleashed_headers(query_string: str) -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-auth-id": UNLEASHED_API_ID,
        "api-auth-signature": sign(query_string),
        "client-type": CLIENT_TYPE,
    }


def parse_ms_date(value):
    if value is None:
        return None
    match = re.search(r"-?\d+", str(value))
    if not match:
        return None
    try:
        dt = datetime.fromtimestamp(int(match.group()) / 1000, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError):
        return None


def safe_get(obj, *keys):
    current = obj
    for k in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(k)
    return current


def get_modified_since_iso() -> str:
    """
    Compute the UTC ISO timestamp for `now - LOOKBACK_DAYS`.
    Unleashed wants UTC; format like '2026-06-19T00:00:00'.
    """
    since = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    # Unleashed accepts ISO 8601 without TZ suffix; strip microseconds for cleanliness
    return since.strftime("%Y-%m-%dT%H:%M:%S")


# ==========================================
# 🔄 TRANSFORMS
# ==========================================

def transform_sales_order(order: dict) -> dict:
    return {
        "Guid": order.get("Guid"),
        "OrderNumber": order.get("OrderNumber"),
        "OrderDate": parse_ms_date(order.get("OrderDate")),
        "RequiredDate": parse_ms_date(order.get("RequiredDate")),
        "CompletedDate": parse_ms_date(order.get("CompletedDate")),
        "ReceivedDate": parse_ms_date(order.get("ReceivedDate")),
        "PaymentDueDate": parse_ms_date(order.get("PaymentDueDate")),
        "CreatedOn": parse_ms_date(order.get("CreatedOn")),
        "LastModifiedOn": parse_ms_date(order.get("LastModifiedOn")),
        "OrderStatus": order.get("OrderStatus"),
        "CustomOrderStatus": order.get("CustomOrderStatus"),
        "SourceId": order.get("SourceId"),
        "SaveAddress": order.get("SaveAddress"),
        "AllocateProduct": order.get("AllocateProduct"),
        "SendAccountingJournalOnly": order.get("SendAccountingJournalOnly"),
        "CreatedBy": order.get("CreatedBy"),
        "LastModifiedBy": order.get("LastModifiedBy"),
        "Customer_CustomerName": safe_get(order, "Customer", "CustomerName"),
        "CustomerRef": order.get("CustomerRef"),
        "SalesPerson_FullName": safe_get(order, "SalesPerson", "FullName"),
        "SalesAccount": order.get("SalesAccount"),
        "SalesOrderGroup": order.get("SalesOrderGroup"),
        "DeliveryName": order.get("DeliveryName"),
        "DeliveryContact": order.get("DeliveryContact"),
        "DeliveryInstruction": order.get("DeliveryInstruction"),
        "DeliveryMethod": order.get("DeliveryMethod"),
        "Warehouse_WarehouseCode": safe_get(order, "Warehouse", "WarehouseCode"),
        "Currency_CurrencyCode": safe_get(order, "Currency", "CurrencyCode"),
        "ExchangeRate": order.get("ExchangeRate"),
        "SubTotal": order.get("SubTotal"),
        "TaxTotal": order.get("TaxTotal"),
        "Total": order.get("Total"),
        "BCSubTotal": order.get("BCSubTotal"),
        "BCTaxTotal": order.get("BCTaxTotal"),
        "BCTotal": order.get("BCTotal"),
        "DiscountRate": order.get("DiscountRate"),
        "TaxRate": order.get("TaxRate"),
        "XeroTaxCode": order.get("XeroTaxCode"),
        "TotalVolume": order.get("TotalVolume"),
        "TotalWeight": order.get("TotalWeight"),
        "Comments": order.get("Comments"),
    }


def transform_sales_order_line(line: dict, parent_order: dict) -> dict:
    return {
        "Guid": line.get("Guid"),
        "SalesOrderGuid": parent_order.get("Guid"),
        "SalesOrderOrderNumber": parent_order.get("OrderNumber"),
        "LineNumber": line.get("LineNumber"),
        "LineType": line.get("LineType"),
        "Product_ProductCode": safe_get(line, "Product", "ProductCode"),
        "Product_ProductDescription": safe_get(line, "Product", "ProductDescription"),
        "DueDate": parse_ms_date(line.get("DueDate")),
        "OrderQuantity": line.get("OrderQuantity"),
        "UnitPrice": line.get("UnitPrice"),
        "DiscountRate": line.get("DiscountRate"),
        "LineTotal": line.get("LineTotal"),
        "Volume": line.get("Volume"),
        "Weight": line.get("Weight"),
        "AverageLandedPriceAtTimeOfSale": line.get("AverageLandedPriceAtTimeOfSale"),
        "TaxRate": line.get("TaxRate"),
        "LineTax": line.get("LineTax"),
        "XeroTaxCode": line.get("XeroTaxCode"),
        "BCUnitPrice": line.get("BCUnitPrice"),
        "BCLineTotal": line.get("BCLineTotal"),
        "BCLineTax": line.get("BCLineTax"),
        "LineTaxCode": line.get("LineTaxCode"),
        "XeroSalesAccount": line.get("XeroSalesAccount"),
        "CostOfGoodsAccount": line.get("CostOfGoodsAccount"),
        "Comments": line.get("Comments"),
        "SerialNumbers": line.get("SerialNumbers"),
        "BatchNumbers": line.get("BatchNumbers"),
        "Assembly": line.get("Assembly"),
    }


# ==========================================
# 🌐 PHASE 1: FETCH (with modifiedSince filter)
# ==========================================

def fetch_sales_orders_page(page: int, modified_since: str) -> dict:
    """
    Fetch one page of Sales Orders modified since the given UTC ISO timestamp.
    """
    # 1. Build the query string manually. 
    # Unleashed HMAC signatures fail if colons are URL-encoded into '%3A'
    query_string = f"modifiedSince={modified_since}&pageSize={PAGE_SIZE}"
    
    path = f"/SalesOrders/Page/{page}" if page > 1 else "/SalesOrders"
    url = f"{UNLEASHED_HOST}{path}?{query_string}"
    
    # Generate headers using the EXACT unencoded query string
    headers = unleashed_headers(query_string)

    response = requests.get(url, headers=headers, timeout=60)
    
    if response.status_code == 429:
        print(f"   ⚠️ Rate limit on page {page}, sleeping 60s and retrying...")
        time.sleep(60)
        response = requests.get(url, headers=headers, timeout=60)

    if response.status_code != 200:
        print(f"❌ Page {page} failed ({response.status_code}): {response.text[:200]}")
        response.raise_for_status()

    return response.json()


def fetch_recent_sales_orders(modified_since: str) -> list:
    """PHASE 1: Pull all Sales Orders modified since the given timestamp."""
    print(f"🌐 PHASE 1 — Fetching Sales Orders modified since {modified_since}...")
    t0 = time.time()

    page1 = fetch_sales_orders_page(1, modified_since)
    pagination = page1.get("Pagination", {})
    total_pages = pagination.get("NumberOfPages", 1)
    total_items = pagination.get("NumberOfItems", 0)

    print(f"   📊 {total_items:,} matching orders across {total_pages} pages")

    if total_items == 0:
        print("   ℹ️  Nothing to sync. Exiting cleanly.")
        return []

    last_page = min(total_pages, MAX_PAGES) if MAX_PAGES else total_pages
    all_orders = list(page1.get("Items", []))
    print(f"   ✅ Page 1/{last_page} → {len(page1.get('Items', []))} orders")

    if last_page > 1:
        remaining = list(range(2, last_page + 1))
        with ThreadPoolExecutor(max_workers=FETCH_CONCURRENCY) as executor:
            future_to_page = {
                executor.submit(fetch_sales_orders_page, p, modified_since): p
                for p in remaining
            }
            done = 1
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    page_data = future.result()
                    all_orders.extend(page_data.get("Items", []))
                    done += 1
                    print(f"   ✅ Page {done}/{last_page} (was page {page_num}) → cumulative {len(all_orders):,} orders")
                except Exception as e:
                    print(f"   ❌ Page {page_num} failed: {e}")
                    raise

    elapsed = time.time() - t0
    print(f"   ⏱️  Fetched {len(all_orders):,} orders in {elapsed:.1f}s")
    return all_orders


# ==========================================
# 🔄 PHASE 2: TRANSFORM
# ==========================================

def transform_all(orders: list) -> tuple[list, list]:
    print("\n🔄 PHASE 2 — Transforming records...")
    t0 = time.time()
    headers = [transform_sales_order(o) for o in orders]
    lines = [
        transform_sales_order_line(line, o)
        for o in orders
        for line in (o.get("SalesOrderLines") or [])
    ]
    elapsed = time.time() - t0
    print(f"   ✅ {len(headers):,} headers + {len(lines):,} line items prepared in {elapsed:.1f}s")
    return headers, lines


# ==========================================
# 📦 PHASE 3: PUSH
# ==========================================

def push_batch(dataset_id: str, records: list, label: str, batch_num: int, total_batches: int) -> tuple[bool, str]:
    url = f"https://api.databox.com/v1/datasets/{dataset_id}/data"
    payload = {"records": records}
    try:
        r = requests.post(url, headers=DATABOX_HEADERS, json=payload, timeout=60)
        if r.status_code == 200:
            res = r.json()
            ingestion_id = res.get("id") or res.get("ingestionId")
            print(f"   ✅ [{label}] batch {batch_num}/{total_batches} ({len(records)} records) → {ingestion_id}")
            return True, ingestion_id
        else:
            err = f"HTTP {r.status_code}: {r.text[:200]}"
            print(f"   ❌ [{label}] batch {batch_num}/{total_batches} failed → {err}")
            return False, err
    except Exception as e:
        print(f"   ❌ [{label}] batch {batch_num}/{total_batches} exception → {e}")
        return False, str(e)


def push_all_to_dataset(records: list, dataset_id: str, label: str) -> list:
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    total = len(batches)
    if total == 0:
        print(f"   ℹ️  [{label}] no records to push")
        return []

    print(f"   📤 [{label}] {len(records):,} records → {total} batches of {BATCH_SIZE}")
    ingestion_ids = []
    with ThreadPoolExecutor(max_workers=PUSH_CONCURRENCY) as executor:
        futures = {
            executor.submit(push_batch, dataset_id, batch, label, i + 1, total): i
            for i, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            ok, ingestion_or_err = future.result()
            if ok:
                ingestion_ids.append(ingestion_or_err)
    return ingestion_ids


def push_all_to_databox(headers: list, lines: list):
    print("\n📦 PHASE 3 — Pushing to Databox (header + line datasets in parallel)...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_headers = executor.submit(push_all_to_dataset, headers, SO_HEADER_DATASET_ID, "header")
        f_lines = executor.submit(push_all_to_dataset, lines, SO_LINE_DATASET_ID, "line")
        header_ids = f_headers.result()
        line_ids = f_lines.result()
    elapsed = time.time() - t0
    print(f"   ⏱️  Pushed in {elapsed:.1f}s")
    print(f"   📊 Header ingestions: {len(header_ids)} | Line ingestions: {len(line_ids)}")


# ==========================================
# 🚀 MAIN
# ==========================================

def main():
    overall_start = time.time()
    print(f"🚀 Unleashed → Databox DAILY SYNC (lookback: {LOOKBACK_DAYS} days)\n")

    modified_since = get_modified_since_iso()
    orders = fetch_recent_sales_orders(modified_since)

    if not orders:
        print(f"\n🎉 Done. No changes since {modified_since}. Wall time: {time.time() - overall_start:.1f}s")
        return

    headers, lines = transform_all(orders)
    push_all_to_databox(headers, lines)

    print(f"\n🎉 Done. Total wall time: {time.time() - overall_start:.1f}s")
    print(f"   {len(headers):,} orders synced, {len(lines):,} line items pushed")


if __name__ == "__main__":
    main()