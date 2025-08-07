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
        
        
        page_num = 1
        target_url = f"https://www.crexi.com/properties?pageSize=60&sort=New%20Listings&showMap=false&types%5B%5D=Industrial&placeIds%5B%5D=ChIJSTKCCzZwQIYRPN4IGI8c6xY&page={page_num}"

        
        # Predicate: be specific enough to avoid matching other calls
        predicate = lambda r: (
            "api.crexi.com/assets/search" in r.url
        )
    

        with page.expect_response(predicate, timeout=30_000) as resp_info:
            page.goto(target_url, wait_until='domcontentloaded')

        response = resp_info.value

        print(response)
        print(response.json())
       
if __name__ == "__main__":
    print("Here we go...")
    listings = scrape_crexi_listings()
   