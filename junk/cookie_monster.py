import asyncio, json, httpx
from playwright.async_api import async_playwright

# --- 1) launch headless browser and let it solve Cloudflare ---
async def get_crexi_cookies():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx     = await browser.new_context()
        page    = await ctx.new_page()

        # any public page will do – the homepage is light
        await page.goto("https://www.crexi.com", wait_until="networkidle")

        # pull cookies for both zones the API cares about
        cookies = await ctx.cookies("https://www.crexi.com",
                                    "https://api.crexi.com")
        await browser.close()

    # httpx wants a {name: value} dict
    return {c["name"]: c["value"] for c in cookies}

# --- 2) hit the search API with those cookies ---
async def fetch_search(offset=0, count=60):
    cookies = await get_crexi_cookies()

    headers = {
        "accept":               "application/json, text/plain, */*",
        "content-type":         "application/json",
        "client-timezone-offset": str(-time.localtime().tm_gmtoff // 60),
        # optional but nice to copy-paste from DevTools:
        "origin":               "https://www.crexi.com",
        "referer":              "https://www.crexi.com/",
        "user-agent":           "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/138.0.0.0 Safari/537.36",
    }

    body = {
        "count":            count,
        "offset":           offset,
        "sortDirection":    "Descending",
        "sortOrder":        "rank",
        "includeUnpriced":  True,
        # these two are optional – omit and the server fills them in,
        # but passing them avoids the “recommendations” behaviour
        "mlScenario":       "Recombee-Recommendations",
        "userId":           cookies.get("mixpanel-distinct-id", "")
    }

    async with httpx.AsyncClient(headers=headers, cookies=cookies,
                                 timeout=30.0) as client:
        r = await client.post(
                "https://api.crexi.com/assets/search",
                json=body,
            )
        r.raise_for_status()
        return r.json()

async def main():
    first_page = await fetch_search(offset=0)
    # pretty-print just the first result so the output isn’t huge
    print(json.dumps(first_page["items"][0], indent=2))

if __name__ == "__main__":
    asyncio.run(main())
