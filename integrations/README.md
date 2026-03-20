# Integrations

Each file in this folder is a standalone ETL script for a specific platform.

## File Naming Convention

`{platform}-v2.py` — The "v2" indicates these are mature, production versions (there were v1s during development).

## Structure

Every integration follows this structure:

```
┌─────────────────────────────────────────┐
│  ⚙️ CONFIGURATION                       │  ← API keys, dataset IDs, date settings
├─────────────────────────────────────────┤
│  🛠️ HELPER FUNCTIONS                    │  ← Date handling, API requests, retries
├─────────────────────────────────────────┤
│  🧠 PRE-FETCH / CACHING                 │  ← Load lookup tables (practitioners, etc.)
├─────────────────────────────────────────┤
│  📦 FETCHERS                            │  ← Pull data from source API
├─────────────────────────────────────────┤
│  🔄 TRANSFORMERS                        │  ← Reshape data for Databox
├─────────────────────────────────────────┤
│  📊 DATABOX PUSH & VERIFY               │  ← Send to Databox, confirm success
├─────────────────────────────────────────┤
│  🚀 MAIN                                │  ← Orchestrates the full ETL flow
└─────────────────────────────────────────┘
```

## Running Locally

```bash
# Set your credentials
export API_TOKEN="your-source-api-token"
export DATABOX_API_KEY="pak_your-key"

# Run for today
python cliniko-v2.py

# Or set date range in the script's CONFIGURATION section for historical sync
```

## Adding a New Integration

1. Copy an existing integration as a template
2. Update the CONFIGURATION section with new API endpoints and dataset IDs
3. Implement fetcher functions for the source API
4. Write transformers to map fields to your Databox dataset schema
5. Test with a single day before running historical backfills
