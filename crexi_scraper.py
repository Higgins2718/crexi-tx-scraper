from playwright.sync_api import sync_playwright
import json
import time
import duckdb
from datetime import date
import subprocess


# TO DO
# Handle issue where scraper doesn't get any listings on page 1 and switches to page 2
'''
Page title: Search Properties | Commercial Real Estate for Sale | Crexi.com

Total listings captured: 0
Navigating to Crexi properties page no. 2
✓ Captured 60 listings (total unique: 60)
Waiting for API response...
Got the data!
'''

DB_PATH = "crexi_tx_industrial.duckdb"

STOP_IDS_DB_PATH = "stop_ids.duckdb"
con2 = duckdb.connect(STOP_IDS_DB_PATH)
# Pull the IDs into Python (use this set to know when to stop next scrape)
first_listing_ids_last_run = {
    r[0] for r in con2.execute("SELECT * FROM stop_ids").fetchall()
}
print("first_listing_ids_last_run:", first_listing_ids_last_run)

con2.close()

def scrape_crexi_listings():
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
        
        # Collect all API responses
        all_listings = []
        got_first_response = False
        found_listings_in_response = False

         # Halt when scraper encounters values already scraped in last week's run
        found_last_week_ids = False        
        listings_counter = 1

        def handle_response(response):
            nonlocal got_first_response, found_last_week_ids, listings_counter, found_listings_in_response, page_num   
            if 'api.crexi.com/assets/search' in response.url:
                try:
                    data = response.json()
                    if 'data' in data:
                        print("WE GOT DATA")
                        new_listings = data['data']

                        if any('id' in listing for listing in data.get('data', [])):
                            found_listings_in_response = True
                            print("FOUND LISTINGS IN RESPONSE")
                        else:
                            print("FOUND NO LISTINGS IN RESPONSE")
                            print(data['data'])
                            found_listings_in_response = False
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
                        got_first_response = True
                except Exception as e:
                    print(f"Error parsing response: {e}")

       
        page_num = 1
        retries = 0
        # Attach response handler
        page.on('response', handle_response)
        while found_last_week_ids == False:
            # Reset flag
            got_first_response = False
            # Deprecated:
            #current_listings_before = len(all_listings)  

            # Navigate to the properties page
            print(f"Navigating to Crexi properties page no. {page_num}")
            page.goto(f"https://www.crexi.com/properties?pageSize=60&sort=New%20Listings&showMap=false&types%5B%5D=Industrial&placeIds%5B%5D=ChIJSTKCCzZwQIYRPN4IGI8c6xY&page={page_num}", wait_until='domcontentloaded')
            
            # Wait for the API response
            print("Waiting for API response...")
            max_wait = 10  # seconds
            for i in range(max_wait):
                if got_first_response:
                    print("Got the data!")
                    break
                time.sleep(1)
               
            # Check if we got any new listings - retry if not
            # Old method.... deprecated
            '''new_listings_count = len(all_listings) - current_listings_before
            
            if new_listings_count == 0:
                retries +=1
                if retries > 3:
                    print(f"Failed to capture new listings after {retries} retries, stopping.")
                    break
                print(f"No listings captured on page {page_num}, retrying...")
                time.sleep(3)  # Wait a bit longer before retry
                
                continue  # Retry the same page'''
            
            '''if found_listings_in_response == False:
                retries += 1
                if retries > 3:
                    print(f"Failed to capture new listings after {retries} retries, stopping.")
                    break
                print(f"No listings captured on page {page_num}, retrying...")
                time.sleep(3)
                continue  # Retry the same page
'''
            # Debug info
            print(f"Current URL: {page.url}")
            print(f"Page title: {page.title()}")
            print(f"\nTotal listings captured: {len(all_listings)}")
            
            
            
            # Update page pointer         
            page_num += 1
            retries = 0
            # Sleep briefly before navigating to next page
            time.sleep(2)

        # Keep browser open for a moment
        print("\nPress Enter to close browser...")
        input()
        
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
        MAIN_DB = "crexi_tx_industrial.duckdb"
        STOP_DB = "stop_ids.duckdb"

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

'''
con = duckdb.connect(db_path)
    before = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    # Insert only those rows whose id isn't already in listings
    '' con.execute(f"""
        INSERT INTO listings
        SELECT *
        FROM read_json_auto('{json_path}')
        WHERE id NOT IN (SELECT id FROM listings)
    """)''

    # This commented out line seems to break the code
    # con.execute("CREATE OR REPLACE TEMP TABLE new_batch AS SELECT * FROM read_json_auto(?)", [json_path])

    # load JSON once for this run
    con.execute("CREATE OR REPLACE TEMP TABLE new_batch AS SELECT * FROM read_json_auto(?)", [json_path])

    # insert matching columns BY NAME
    con.execute("""
        INSERT INTO listings BY NAME
        SELECT * FROM new_batch
        WHERE id NOT IN (SELECT id FROM listings)
    """)

    after = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    print(f"Inserted {after - before} new listings.")

    # Total number of distinct IDs
    distinct_count = con.execute("SELECT COUNT(DISTINCT id) FROM listings").fetchone()[0]
    print(f"Total rows: {after}")
    print(f"Distinct IDs: {distinct_count}")
    con.close()

'''
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