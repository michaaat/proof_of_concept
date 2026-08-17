# Databox Custom Integrations

A collection of Python ETL scripts that sync data from various SaaS platforms into [Databox](https://databox.com) datasets. Built over 11 months of customer integrations work.

## What This Is

These are **production scripts** deployed via GitHub Actions for Databox customers. They're not academic exercises or theoretical implementations — they run daily and move real data.

Each script follows the same pattern:
1. **Extract** — Fetch data from source API (handling auth, pagination, rate limits)
2. **Transform** — Reshape records into Databox-ready format
3. **Load** — Push to Databox datasets with verification

## Integrations

| Platform | Type | What It Syncs |
|----------|------|---------------|
| [Cliniko](integrations/cliniko-v2.py) | Healthcare Practice Management | Patients, referrals, bookings, invoices, invoice items, stock adjustments |
| [ServiceTitan](integrations/servicetitan-v2.py) | Field Service Management | Multiple reports (leads, jobs, estimates, backlog) via reporting API |
| [ShipStation](integrations/shipstation-v2.py) | Shipping/Fulfillment | Orders, order line items, shipments (created + modified) |
| [Freshcaller](integrations/freshcaller-v2.py) | Call Center | Call logs and metrics |
| [Connecteam](integrations/connecteam-v2.py) | Workforce Management | Time tracking, shifts, employee data |
| [NutshellCRM](integrations/nutshellcrm-v2.py) | CRM | Leads, activities, pipeline data |
| [MomentumCRM](integrations/momentumcrm-v2.py) | CRM | Contact and deal records |
| [PermaLeads](integrations/permaleads-v2.py) | Lead Generation | Lead records and conversions |
| [Unleashed](integrations/unleashed-v2.py) | Inventory / ERP | Sales order headers and line items (incremental, HMAC-signed) |

## Common Features

Every script includes:

- **Rate limit handling** — Respects API limits with automatic backoff and retry
- **Pagination** — Fetches all records across multiple pages
- **Error handling** — Retries on failures, logs issues, doesn't crash silently
- **Date range modes** — Run for today (daily cron) or historical backfill
- **Data enrichment** — Resolves linked records (e.g., practitioner IDs → names)
- **Chunked uploads** — Splits large datasets to stay under Databox limits
- **Ingestion verification** — Confirms data landed successfully

## How They're Used

These scripts run as GitHub Actions on customer repositories:

```yaml
# Example: Daily sync at 6 AM
on:
  schedule:
    - cron: '0 6 * * *'

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install requests pytz
      - run: python cliniko-v2.py
```

## Configuration

Each script has a configuration section at the top:

```python
# ==========================================
# ⚙️ CONFIGURATION
# ==========================================

# Date settings (None = today, strings = historical range)
HISTORICAL_START_DATE = None
HISTORICAL_END_DATE = None

# API credentials
API_TOKEN = "your-token-here"

# Databox settings
DATABOX_API_KEY = "pak_xxx"
DATABOX_DATASET_ID = "abc123"
```

> **Note:** In production deployments, credentials are stored in GitHub Secrets and injected via environment variables. The hardcoded values here are placeholders.

## Dependencies

```
requests
pytz
```

Some scripts also use:
- `dataclasses` (ServiceTitan — for report configuration)
- Standard library only otherwise

## A Note on Code Quality

This isn't over-engineered framework code. It's practical, readable, and it works. Each script is self-contained — you can understand what it does by reading top to bottom.

Things I intentionally didn't do:
- No abstract base classes or complex inheritance
- No external ORM or framework dependencies  
- No configuration files that require documentation to understand
- No "clean architecture" patterns that add indirection without value

Things I did do:
- Clear section headers with emoji (easy to navigate)
- Consistent structure across all scripts
- Meaningful variable names
- Comments where the "why" isn't obvious

## License

These scripts were built for customer deployments. Shared here as a portfolio of integration work.

---

*Built at Databox, 2025-2026*
