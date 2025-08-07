# src/playwright_boot.py
import os, json, asyncio, random
from dotenv import load_dotenv
from rich import print as rprint
from playwright.async_api import async_playwright
from playwright_stealth import Stealth          # ← correct import

load_dotenv()
PROXIES = [p for p in os.getenv("PROXY_LIST", "").split(",") if p]


def build_proxy():
    """Return Playwright proxy dict or None."""
    if not PROXIES:
        return None
    cred, host_port = PROXIES[0].split("@", 1)
    user, pwd = cred.split(":", 1)
    host, port = host_port.split(":")
    return {
        "server": f"http://{host}:{port}",
        "username": user,
        "password": pwd
    }

async def main():
    stealth = Stealth()

    # Wrap playwright in stealth to apply all anti-bot patches
    async with stealth.use_async(async_playwright()) as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy=build_proxy()
        )
        ctx = await browser.new_context()
        page = await ctx.new_page()

        rprint("[cyan]→ Navigating to CREXI properties…[/cyan]")
        await page.goto("https://www.crexi.com/properties", timeout=60000)

        # 1) Wait for the SPA to redirect to the real listings view
        await page.wait_for_url("**/properties?pageSize=60", timeout=15000)
        rprint("[green]✔ Reached /properties?pageSize=60[/green]")

     
        # 3) Short buffer to allow final scripts to settle
        await page.wait_for_timeout(1000)

        # 4) Dump cookies + UA
        state = await ctx.storage_state()
        with open("cookies.json", "w") as f:
            json.dump(state, f)

        # 5) Confirm
        rprint(f"[green]✓ Saved cookies.json ({len(state['cookies'])} cookies)[/green]")
        ua = state.get("origins", [{}])[0].get("userAgent", "n/a")
        rprint("[yellow]User-Agent:[/yellow]", ua)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())