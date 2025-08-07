from playwright.sync_api import sync_playwright
import json
import time
import duckdb
from datetime import date
import subprocess



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
        
                            all_listings.append(listing)
                            
                            listings_counter += 1

                        print(f"✓ Captured {len(new_listings)} listings (total unique: {len(all_listings)})")
                        got_first_response = True
                except Exception as e:
                    print(f"Error parsing response: {e}")

       
        page_num = 1
        retries = 0
        # Attach response handler
        page.on('response', handle_response)
        #while found_last_week_ids == False:
        # Reset flag
        got_first_response = False
        

        # Navigate to the properties page
        print(f"Navigating to Crexi properties page no. {page_num}")
        page.goto(f"https://www.crexi.com/properties?pageSize=60&sort=New%20Listings&showMap=false&types%5B%5D=Industrial&placeIds%5B%5D=ChIJSTKCCzZwQIYRPN4IGI8c6xY&page={page_num}", wait_until='domcontentloaded')
        


        # Debug info
        print(f"Current URL: {page.url}")
        print(f"Page title: {page.title()}")
        print(f"\nTotal listings captured: {len(all_listings)}")
        
        
        
        # Update page pointer         
        #page_num += 1
        retries = 0
        # Sleep briefly before navigating to next page
        time.sleep(2)

        # Keep browser open for a moment
        print("\nPress Enter to close browser...")
        input()
        
        browser.close()
        
        return all_listings
   


if __name__ == "__main__":
    print("Here we go...")
    listings = scrape_crexi_listings()
   