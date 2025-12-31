from playwright.async_api import async_playwright
import asyncio
from adapter.punch import parse_punch_news
from adapter.channeltv import parse_channel_news
import time

async def main():
    start = time.time()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        data_url = [
            "https://punchng.com",
            "https://www.channelstv.com/",
        ]

        for index, url in enumerate(data_url):
            await page.goto(url, wait_until="domcontentloaded")
            content = await page.content()
        await browser.close()
        if index == 0:
                parsed = parse_punch_news(content)
        elif index == 1:
                parse_channel_news(content)
        end = time.time()
        print(f"Time Taken: {end - start}  seconds")  

asyncio.run(main())

