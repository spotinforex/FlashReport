import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from playwright.async_api import async_playwright
import asyncio
from adapter.punch import parse_punch_news
from adapter.channeltv import parse_channel_news
from adapter.vanguard import parse_vanguard_news
from adapter.premuimtimes import parse_premuimtimes_news
import time

async def main():
    try:
        start = time.time()
        logging.info("Scraping Process Initialized")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            data_url = [
                "https://punchng.com",
                "https://www.channelstv.com/",
                "https://www.vanguardngr.com/",
                "https://www.premiumtimesng.com/",
            ]

            for index, url in enumerate(data_url):
                await page.goto(url, wait_until="domcontentloaded")
                content = await page.content()

                if index == 0:
                    parsed = parse_punch_news(content)
                elif index == 1:
                    parsed = parse_channel_news(content)
                elif index == 2:
                    parsed = parse_vanguard_news(content)
                elif index == 3:
                    parsed = parse_premuimtimes_news(content)

            await browser.close()

        end = time.time()
        logging.info(f"Time Taken: {end - start} seconds")
    except Exception as e:
        logging.error(f" An Error Occurred In The Scraping Data Process: {e}")
        return None
asyncio.run(main())

