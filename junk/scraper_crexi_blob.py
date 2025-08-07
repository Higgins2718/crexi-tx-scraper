from playwright.sync_api import sync_playwright
import json
import time
import duckdb

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
        seen_ids = set()
        got_first_response = False
        
        def handle_response(response):
            nonlocal got_first_response
            if 'api.crexi.com/assets/search' in response.url:
                try:
                    data = response.json()
                    if 'data' in data:
                        new_listings = data['data']
                        for listing in new_listings:
                            if listing['id'] not in seen_ids:
                                seen_ids.add(listing['id'])
                                all_listings.append(listing)
                        print(f"✓ Captured {len(new_listings)} listings (total unique: {len(all_listings)})")
                        got_first_response = True
                except Exception as e:
                    print(f"Error parsing response: {e}")

        # Define num of pages to scrape to initialize db            
        pages_to_scrape = 2
        page_num = 1
        # Attach response handler
        page.on('response', handle_response)
        while page_num <= pages_to_scrape:
            # Reset flag
            got_first_response = False

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
            
            # Debug info
            print(f"Current URL: {page.url}")
            print(f"Page title: {page.title()}")
            print(f"\nTotal listings captured: {len(all_listings)}")
            
            
            
            # Update page pointer         
            page_num += 1
            # Sleep briefly before navigating to next page
            time.sleep(2)

        # Keep browser open for a moment
        print("\nPress Enter to close browser...")
        input()
        
        browser.close()
        
        return all_listings
    
def save_listings_to_db(listings):

    if listings:
        # Save to JSON file
        with open('crexi_tx_industrial.json', 'w') as f:
            json.dump(listings, f, indent=2)
        print("✓ Saved to crexi_tx_industrial.json")

        # Save to DuckDB
        conn = duckdb.connect('crexi_tx_industrial.duckdb')

        # Create table from JSON
        conn.execute("""
            CREATE OR REPLACE TABLE listings AS 
            SELECT * FROM read_json_auto('crexi_tx_industrial.json')
        """)

        # Verify it worked
        count = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        print(f"✓ Saved {count} listings to crexi_tx_industrial.duckdb")

        conn.close()
    else:
        print("❌ No listings captured")


if __name__ == "__main__":
    listings = scrape_crexi_listings()
    save_listings_to_db(listings)
    print(f"\n✅ Successfully scraped {len(listings)} listings from Crexi!")