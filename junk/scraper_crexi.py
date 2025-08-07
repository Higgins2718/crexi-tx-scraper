import os
import json
import asyncio

import duckdb
import pyarrow as pa

from playwright.async_api import async_playwright

# — CONFIG —
COOKIES_FILE = "cookies.json"
OUTPUT_DB   = "crexi_day2.duckdb"
SEARCH_URL  = "https://services.crexi.com/search"
PAYLOAD = {
    "assetTypes": ["industrial"],
    "from": 0,
    "size": 200
}
# — END CONFIG —

async def main():
    # 1) Launch browser, load session from Day-1
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(storage_state=COOKIES_FILE)
        
        # 2) Fire the GraphQL/search POST
        resp = await ctx.request.post(
            SEARCH_URL,
            data=PAYLOAD
        )
        resp.raise_for_status()
        data = await resp.json()
        await browser.close()

    hits = data.get("hits", [])
    print(f"Retrieved {len(hits)} listings")

    # 3) Flatten into rows
    rows = []
    for h in hits:
        loc = h.get("location", {})
        broker = h.get("broker", {})
        rows.append({
            "id":             h.get("id"),
            "title":          h.get("title"),
            "address":        loc.get("address"),
            "city":           loc.get("city"),
            "state":          loc.get("state"),
            "zip":            loc.get("zipCode"),
            "price":          h.get("price"),
            "cap_rate":       h.get("capRate"),
            "size_sqft":      h.get("size"),
            "broker_name":    broker.get("name"),
            "broker_phone":   broker.get("phone"),
            "broker_email":   broker.get("email"),
        })

    # 4) Write to DuckDB
    table = pa.Table.from_pylist(rows)
    conn = duckdb.connect(OUTPUT_DB)
    conn.execute("DROP TABLE IF EXISTS crexi_listings")
    conn.register("incoming", table)
    conn.execute("""
        CREATE TABLE crexi_listings AS
        SELECT * FROM incoming
    """)
    conn.close()

    print(f"Wrote {len(rows)} rows to {OUTPUT_DB}")

if __name__ == "__main__":
    asyncio.run(main())
