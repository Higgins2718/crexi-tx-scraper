import duckdb, requests, urllib.parse, json
con = duckdb.connect("crexi_data.duckdb")

def strip_county(addr: str) -> str:
    # ["869 Bryan Rd, O'Fallon", " St. Charles County", " MO 63366"]
    a, _, c = addr.rsplit(",", 2)
    return f"{a.strip()}, {c.strip()}"


for listing_id, address in con.execute("""
        SELECT id,
               locations[1].fullAddress AS address
        FROM   listings
        WHERE  locations[1].fullAddress IS NOT NULL
    """).fetchall():
    # ‑‑ call your Zillow‑scraper here ‑‑
    # zillow_scrape(address)
    print(listing_id, address)


addr = strip_county("3993 US-90, Del Rio, Val Verde County, TX 78840")
url  = f"https://www.zillowstatic.com/autocomplete/v3/suggestions?q={urllib.parse.quote_plus(addr)}"
#zpid = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).json()["results"][0]["metaData"]["zpid"]
zpid = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).json()

print(zpid)

