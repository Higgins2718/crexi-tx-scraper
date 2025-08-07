from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PWTimeout

import json
import time
import duckdb
from datetime import date
import subprocess


LISTINGS_DB_PATH = "databases/crexi_tx_industrial.duckdb"

STOP_IDS_DB_PATH = "databases/stop_ids.duckdb"

def get_stop_ids():
    con = duckdb.connect(STOP_IDS_DB_PATH)
    # Pull the IDs into Python (use this set to know when to stop next scrape)
    stop_ids = {
        r[0] for r in con.execute("SELECT * FROM stop_ids").fetchall()
    }
    con.close()

    return stop_ids


def fetch_results(page, target_url, predicate):
    with page.expect_response(predicate, timeout=30_000) as resp_info:
        page.goto(target_url, wait_until='domcontentloaded')

    response = resp_info.value
    # <Response url='https://api.crexi.com/assets/search' request=<Request url='https://api.crexi.com/assets/search' method='POST'>>
    data = response.json()
    if any('id' in listing for listing in data.get('data', [])):
        print(data['data'][0])
        listings = data['data']
        return listings

    else:
        raise RuntimeError("didn't get data")
    
def scrape_crexi_listings():
    stop_ids = get_stop_ids()
    print(stop_ids)
    # New listings to insert in Listings DB
    new_listings = []
    
    with sync_playwright() as p:
        # Launch browser with visible window for debugging
        browser = p.chromium.launch(
            headless=False,  # Keep this False for now to see what's happening
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        # Enable console logging
        page.on("console", lambda msg: print(f"Console: {msg.text}"))
        
        page_num = 1
        listings_counter = 0
        
        # Deprecated
        # stopping_condition = False
        '''if any('id' in listing for listing in data.get('data', [])):
                            found_listings_in_response = True'''
        while True:
            
            print(f"pagenum {page_num}")
            target_url = f"https://www.crexi.com/properties?pageSize=60&sort=New%20Listings&showMap=false&types%5B%5D=Industrial&placeIds%5B%5D=ChIJSTKCCzZwQIYRPN4IGI8c6xY&page={page_num}"

            
            # Predicate: be specific enough to avoid matching other calls
            predicate = lambda r: (
                "api.crexi.com/assets/search" in r.url
                and r.request.method == "POST"
            )

            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                try:
                    url = target_url if attempt == 1 else f"{target_url}&ts={int(time.time()*1000)}"
                    listings = fetch_results(page, url, predicate)
                    break
                except (PWTimeout, RuntimeError) as e:
                    if attempt == max_attempts:
                        raise RuntimeError(f"Failed after {max_attempts} attempts") from e
                    print(f"Response didn't return listings. Retrying for attempt no. {attempt + 1}")
                    page.wait_for_timeout(1000 * attempt)


            if listings:
                print(f"Successfully received {len(listings)} listings")
                for listing in listings:
                    listings_counter += 1

                    #print(f"Listing {listing['id']} at position #{listings_counter}")

                    if listing['id'] not in stop_ids:
                        new_listings.append(listing)
                    else:
                        
                        print(f"*** Stop condition hit on page {page_num} ***")
                        print(f"Found already-scraped ID on listing #{listings_counter}")
                        return new_listings
            

            # Paginate if needed
            page_num += 1

    
            
    
def save_listings_to_json(listings):

    if not listings:
        print("No new listings to save.")
        return None
    # Save to JSON file
    
    today_str = date.today().isoformat()  # e.g. "2025-07-23"
    filename  = f"crexi_tx_industrial_{today_str}.json"

    with open(filename, 'w') as f:
        json.dump(listings, f, indent=2)

    print(f"✓ Saved to {filename}")

    return filename


def insert_new_listings(json_path, db_path=LISTINGS_DB_PATH):
    try:
        con = duckdb.connect(db_path)
        before = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

        # Load JSON once for this run
        con.execute("CREATE OR REPLACE TEMP TABLE new_batch AS SELECT * FROM read_json_auto(?)", [json_path])
        
        # NEW: Dynamic schema detection and column addition
        new_columns = con.execute("PRAGMA table_info('new_batch')").fetchall()
        existing_columns = con.execute("PRAGMA table_info('listings')").fetchall()
        
        existing_col_names = {col[1] for col in existing_columns}
        new_col_names = {col[1] for col in new_columns}
        missing_columns = new_col_names - existing_col_names
        
        for col_name in missing_columns:
            col_info = next(col for col in new_columns if col[1] == col_name)
            col_type = col_info[2]
            print(f"Adding new column: {col_name} ({col_type})")
            con.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
        
        # YOUR ORIGINAL: Insert matching columns BY NAME
        con.execute("""
            INSERT INTO listings BY NAME
            SELECT * FROM new_batch
            WHERE id NOT IN (SELECT id FROM listings)
        """)

        # YOUR ORIGINAL: Reporting
        after = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        print(f"Inserted {after - before} new listings.")
        
        distinct_count = con.execute("SELECT COUNT(DISTINCT id) FROM listings").fetchone()[0]
        print(f"Total rows: {after}")
        print(f"Distinct IDs: {distinct_count}")
        con.close()

        return True
    except Exception as e:
        print(f"Error inserting new listings: {e}")
        return False
    
def update_stop_ids():
    try:
        MAIN_DB = "databases/crexi_tx_industrial.duckdb"
        STOP_DB = "databases/stop_ids.duckdb"

        ids = [r[0] for r in duckdb.connect(MAIN_DB).execute(
            "SELECT id FROM listings ORDER BY activatedOn DESC LIMIT 60"
        ).fetchall()]

        con = duckdb.connect(STOP_DB)
        con.execute("CREATE OR REPLACE TABLE stop_ids (id BIGINT)")
        con.executemany("INSERT INTO stop_ids VALUES (?)", [(i,) for i in ids])
        con.close()

        print(f"Updated stop_ids with {len(ids)} rows.")
        return True
    
    except Exception as e:
        print(f"Error updating stop_ids: {e}")
        return False


if __name__ == "__main__":
    print("Here we go...")
    new_listings = scrape_crexi_listings()
    #print(f"Newest listings: {new_listings[0]}")
    print(f"Got {len(new_listings)} new listings")
    

    if new_listings is not None:
        json_path = save_listings_to_json(new_listings)
        success = insert_new_listings(json_path, LISTINGS_DB_PATH)

        if success:
            print("Running update stop_ids script...")
            try:
                update_stop_ids()
                print("Update stop_ids script completed successfully")
            except subprocess.CalledProcessError as e:
                print(f"Update stop_ids script failed: {e}")
        else:
            print("Database insertion failed, skipping post-insertion script")


    else:
        print("No new listings to save.")