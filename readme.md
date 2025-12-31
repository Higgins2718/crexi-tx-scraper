# Crexi Industrial Listings Scraper (TX)

Scrapes newly posted Industrial property listings from Crexi (Texas) by intercepting the site’s search API response via Playwright, then persists results into DuckDB with deduping and basic schema evolution.

This is designed to be run repeatedly. On the first run it bootstraps the database, and on later runs it stops once it hits listings that were already seen in the previous run.

## What it does

- Navigates Crexi search result pages for Industrial listings in Texas (fixed query in the script).
- Waits for the `https://api.crexi.com/assets/search` response and parses the JSON payload.
- Collects only listings that are not in the prior run’s `stop_ids` set.
- Writes the newly collected listings to a dated JSON file.
- Inserts new listings into a DuckDB table (`listings`) by ID, avoiding duplicates.
- Updates a separate DuckDB database (`stop_ids.duckdb`) with the most recent 60 listing IDs, used as a stop condition on the next run.

## How it works (high level)

- Scraping: uses `page.expect_response(...)` to capture the Crexi search API response while navigating result pages.
- Incremental stopping: maintains a small set of previously-seen IDs (`stop_ids`) and stops scraping when those IDs appear again.
- Reliability: bounded retries per page if the API response is not observed or the JSON is malformed.
- Storage: DuckDB is used as the local database. Inserts are idempotent by ID.
- Schema evolution: if new fields appear in Crexi JSON, the script automatically adds missing columns to the DuckDB `listings` table before inserting.

## Requirements

- Python 3.10+ recommended
- Playwright
- DuckDB

## Setup

1. Create and activate a virtual environment (optional but recommended):

- Windows (PowerShell):
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

- macOS/Linux:
```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install Python dependencies:

```bash
pip install playwright duckdb
```
3. Install Playwright browsers:
```bash
playwright install
```

4. Ensure the database directory exists:

 ```bash
Copy code
mkdir -p databases
```
# Running
bash
Copy code
python crexi_scraper.py
Output includes:

Console logs from the page

Retry attempts (if needed)

Count of listings captured

JSON file saved (if any new listings)

Number of new rows inserted into DuckDB

stop_ids refresh confirmation

By default the script runs with a visible browser window (headless=False) to make debugging easy.

Files and data
databases/crexi_tx_industrial.duckdb

Contains table listings

Stores all historical listings seen by this scraper

databases/stop_ids.duckdb

Contains table stop_ids

Stores the most recent 60 listing IDs from the main DB (used as a stop condition)

crexi_tx_industrial_YYYY-MM-DD.json

JSON dump of newly collected listings for that run

First run vs subsequent runs
First run:

If stop_ids.duckdb or the stop_ids table does not exist, the script treats it as a first run.

It will scrape up to MAX_PAGES_FIRST_RUN pages (currently 20) as a safety cap.

It bootstraps the listings table in the main DuckDB database using the JSON schema from that run.

Subsequent runs:

Loads stop_ids into memory at startup.

Stops scraping when it encounters an already-seen ID from the previous run.

Inserts only new IDs into listings.

## Notes on idempotency

Re-running the script does not duplicate rows in the DuckDB listings table because inserts are filtered by id.

stop_ids is rebuilt on every successful run (overwritten with the latest 60 IDs).

## Configuration

The query is currently hardcoded in the script:

Place: Texas (via placeIds[])

Type: Industrial

Page size: 60

Sort: New Listings

Tunable constants in the script:

MAX_PAGES_FIRST_RUN (default 20)

timeout for expect_response (default 15000 ms)

Retry cap per page (default 5)

Sleep delay between pages (default 2 seconds)

## Caveats

This is a personal project scraper and is not intended to be a hosted production service.

Crexi may change endpoints, parameters, or response formats over time.

The script currently runs in non-headless mode, but if you want to run it headless xvfb will do the trick.

# Example DuckDB queries

## Count total listings:

```sql
SELECT COUNT(*) FROM listings;
```
## Most recent listings by activation time:

```sql
SELECT id, activatedOn
FROM listings
ORDER BY activatedOn DESC
LIMIT 20;```

## Count distinct IDs (sanity check):
```sql
SELECT COUNT(DISTINCT id) FROM listings;
```