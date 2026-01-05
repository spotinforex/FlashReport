import logging
import sys
import asyncio
import time
from playwright.async_api import async_playwright

from adapter.punch import parse_punch_news
from adapter.channeltv import parse_channel_news
from adapter.vanguard import parse_vanguard_news
from adapter.premuimtimes import parse_premuimtimes_news
from adapter.businessday import parse_businessday_news
from adapter.saharareporters import parse_saharareporters_news
from adapter.guardian import parse_guardian_news
from adapter.arise import parse_arise_news
from datetime import datetime

from database import Database
from preprocessor import preprocessor

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


async def main():
    try:
        start = time.time()
        logging.info("Data Scraping Initialized")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            data_url = [
                "https://punchng.com",
                "https://www.channelstv.com/",
                "https://www.vanguardngr.com/",
                "https://www.premiumtimesng.com",
                "https://businessday.ng/",
                "https://saharareporters.com/news",
                "https://guardian.ng/",
                "https://www.arise.tv/",
            ]

            source_id = {
                'Punch Nigeria': '431e3d96-3927-4481-be94-db8d6f2f9f5b',
                'Channels TV': 'b62f770e-7a5e-48a0-8187-b9e6477fe453',
                'Vanguard Nigeria': '0bdb95b0-c023-4ef1-9328-af977afce3bc',
                'Premium Times Nigeria': '74eea32e-d6a4-4688-81ff-126badd873c0',
                'BusinessDay Nigeria': 'd9fb80d4-4837-47eb-bf72-7d04e7d70c76',
                'Sahara Reporters': '6ce1b673-431c-4a11-9749-47643ca3a96f',
                'The Guardian Nigeria': '1f55e458-834b-4807-b209-004f0690f5f3',
                'Arise News TV': '7b907be0-59ce-4cba-b0d0-ef58034ea8f3'
            }

            parsers = [
                parse_punch_news,
                parse_channel_news,
                parse_vanguard_news,
                parse_premuimtimes_news,
                parse_businessday_news,
                parse_saharareporters_news,
                parse_guardian_news,
                parse_arise_news
            ]

            database = Database()

            for idx, url in enumerate(data_url):
                logging.info(f"Scraping {url}")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                content = await page.content()

                parsed = parsers[idx](content)
                data = preprocessor(parsed)
                articles = data.get("articles", [])

                # Correct source_id mapping
                source_name = list(source_id.keys())[idx]
                for article in articles:
                    article['source_id'] = source_id[source_name]
                    article['scraped_at'] = datetime.now().isoformat()

                if articles:
                    database.insert('parsed_articles', articles)
                    database.update("sources", source_id[source_name], data.get('scraped_at', time.strftime("%Y-%m-%dT%H:%M:%S")))

            await browser.close()

        end = time.time()
        logging.info(f"Time Taken: {end - start:.2f} seconds")

    except Exception as e:
        logging.error(f"An Error Occurred During Data Scraping: {e}")


if __name__ == "__main__":
    asyncio.run(main())

