from playwright.sync_api import sync_playwright
import json
import time
import duckdb
from datetime import date
import subprocess


DB_PATH = "databases/crexi_tx_industrial.duckdb"

STOP_IDS_DB_PATH = "databases/stop_ids.duckdb"
first_listing_ids_last_run = set()
first_run = True  # assume first run until proven otherwise

try:
    con2 = duckdb.connect(STOP_IDS_DB_PATH)
    # Pull the IDs into Python (use this set to know when to stop next scrape)
    first_listing_ids_last_run = {
        r[0] for r in con2.execute("SELECT * FROM stop_ids").fetchall()
    }
    print("first_listing_ids_last_run:", first_listing_ids_last_run)

    con2.close()
    if first_listing_ids_last_run:
        first_run = False
    else:
        first_run = True

except Exception as e:
    print(f"Stop-ids bootstrap: no prior stop_ids table yet ({e}). Treating as first run.")
    first_listing_ids_last_run = set()
    first_run = True

def is_search_response(response):
    return 'api.crexi.com/assets/search' in response.url

def scrape_crexi_listings():
    with sync_playwright() as p:
        # Launch browser with visible window for debugging
        browser = p.chromium.launch(
            headless=False,  # Keep this False
            args=['--disable-blink-features=AutomationControlled']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        # Enable console logging
        page.on("console", lambda msg: print(f"Console: {msg.text}"))
        
        # Collect all API responses
        all_listings = []
        
    
        MAX_PAGES_FIRST_RUN = 20
         # Halt when scraper encounters values already scraped in last week's run
        found_last_week_ids = False        
        listings_counter = 1
        page_num = 1       
        retries = 0

        while (not found_last_week_ids) and retries < 5 and (not first_run or page_num <= MAX_PAGES_FIRST_RUN):

            url = f"https://www.crexi.com/properties?pageSize=60&sort=New%20Listings&showMap=false&types%5B%5D=Industrial&placeIds%5B%5D=ChIJSTKCCzZwQIYRPN4IGI8c6xY&page={page_num}"
            print(f"Navigating to Crexi properties page no. {page_num}")


            try:
                with page.expect_response(is_search_response, timeout=15000) as resp_info:
                    # Navigate to the properties page
                    page.goto(url, wait_until='domcontentloaded')

                # by this point the response has def arrived

                resp = resp_info.value    
                print("Received response")

                if 'api.crexi.com/assets/search' in resp.url:
                    try:
                        data = resp.json()

                        print("json is in url")
                        if 'data' in data:
                            print("Received JSON data")
                            new_listings = data['data']

                            for listing in new_listings:
                
                                if listing['id'] not in first_listing_ids_last_run:
                                    all_listings.append(listing)
                                else:
                                    if not found_last_week_ids:  # Only print once when condition first changes
                                        print(f"*** Stop condition hit on page {page_num} ***")
                                    print(f"Not scraping listing {listings_counter} since it was already scraped last time.")
                                    found_last_week_ids = True
                                listings_counter += 1

                            print(f"✓ Captured {len(new_listings)} listings (total unique: {len(all_listings)})")
                            # Update page pointer         
                            page_num += 1
                            # Reset retry counter if needed
                            retries = 0

                        else:

                            print("issue with json")
                            retries += 1

                    except Exception as e:
                        print(f"Error parsing response: {e}")
                        retries += 1

                else:
                    print("json isn't in url")
                    retries += 1


            except Exception as e:
                print(f"No search response for page {page_num} within timeout {e}")
                # This is the flag that triggers when the scraper fails to get listings for a page
                retries += 1
        
            # Debug info
            print(f"Current URL: {page.url}")
            print(f"Page title: {page.title()}")
            print(f"\nTotal listings captured: {len(all_listings)}")
            
            
            
            
            # Sleep briefly before navigating to next page
            time.sleep(2)

        # Optionally keep browser open for a moment while debugging, otherwise comment next two lines out
        # print("\nPress Enter to close browser...")
        # input()
        
        browser.close()
        
        return all_listings
    
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


def insert_new_listings(json_path, db_path=DB_PATH):

    if not json_path:
        print("No JSON path provided, skipping DB insert.")
        return False
    

    try:
        con = duckdb.connect(db_path)
        # First run bootstrap: create listings table if missing (empty, inferred schema from JSON)
        con.execute("""
            CREATE TABLE IF NOT EXISTS listings AS
            SELECT * FROM read_json_auto(?) WHERE 1=0
        """, [json_path])
        before = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

        # Load JSON once for this run
        con.execute("CREATE OR REPLACE TEMP TABLE new_batch AS SELECT * FROM read_json_auto(?)", [json_path])
        
        # Dynamic schema detection and column addition
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
        
        # Insert matching columns BY NAME
        con.execute("""
            INSERT INTO listings BY NAME
            SELECT * FROM new_batch
            WHERE id NOT IN (SELECT id FROM listings)
        """)

        # Reporting
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
    listings = scrape_crexi_listings()
    if listings is not None:
        json_path = save_listings_to_json(listings)
        
        success = insert_new_listings(json_path, DB_PATH)

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