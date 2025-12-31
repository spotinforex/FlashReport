from playwright.async_api import async_playwright 
import  asyncio
from adapter.punch import parse_punch_news, save_to_json

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto("https://punchng.com", wait_until = "domcontentloaded")
        content = await page.content()

        parsed = parse_punch_news(content)
        save_to_json(parsed)

        await browser.close()

asyncio.run(main())
