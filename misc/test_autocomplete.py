import requests, urllib.parse, json
addr = "7-Timberview-Rd-Lemont-IL-60439"
url  = f"https://www.zillowstatic.com/autocomplete/v3/suggestions?q={urllib.parse.quote_plus(addr)}"
zpid = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).json()["results"][0]["metaData"]["zpid"]
print(zpid)

