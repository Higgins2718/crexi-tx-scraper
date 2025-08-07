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
        
        # Attach response handler
        page.on('response', handle_response)
        
        # Navigate to the properties page
        print("Navigating to Crexi properties page...")
        page.goto('https://www.crexi.com/properties', wait_until='domcontentloaded')
        
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
        
        if all_listings:
            # Save to JSON file
            with open('crexi_listings.json', 'w') as f:
                json.dump(all_listings, f, indent=2)
            print("✓ Saved to crexi_listings.json")
            
            # Save to DuckDB
            conn = duckdb.connect('crexi_data.duckdb')
            
            # Create table from JSON
            conn.execute("""
                CREATE OR REPLACE TABLE listings AS 
                SELECT * FROM read_json_auto('crexi_listings.json')
            """)
            
            # Verify it worked
            count = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
            print(f"✓ Saved {count} listings to crexi_data.duckdb")
            
            # Show a sample
            print("\nSample listings:")
            samples = conn.execute("""
                SELECT 
                    id, 
                    name, 
                    askingPrice,
                    json_extract_string(locations[0], '$.city') as city,
                    json_extract_string(locations[0], '$.state.code') as state
                FROM listings 
                LIMIT 3
            """).fetchall()
            
            for sample in samples:
                price = f"${sample[2]:,.0f}" if sample[2] else "No price"
                print(f"  ID {sample[0]}: {sample[1][:40]}... | {price} | {sample[3]}, {sample[4]}")
            
            conn.close()
        else:
            print("❌ No listings captured")
        
        # Keep browser open for a moment
        print("\nPress Enter to close browser...")
        input()
        
        browser.close()
        
        return all_listings

if __name__ == "__main__":
    listings = scrape_crexi_listings()
    print(f"\n✅ Successfully scraped {len(listings)} listings from page 1!")