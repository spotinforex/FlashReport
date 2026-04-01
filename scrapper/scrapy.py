import logging
import sys
import time
import httpx
import asyncio
from scrapper.adapter.punch import parse_punch_news
from scrapper.adapter.channeltv import parse_channel_news
from scrapper.adapter.vanguard import parse_vanguard_news
from scrapper.adapter.premuimtimes import parse_premuimtimes_news
from scrapper.adapter.businessday import parse_businessday_news
from scrapper.adapter.saharareporters import parse_saharareporters_news
from scrapper.adapter.guardian import parse_guardian_news
from scrapper.adapter.arise import parse_arise_news
from datetime import datetime
from scrapper.database import Database
from scrapper.preprocessor import preprocessor
from scrapper.telegram import scrape_recent_messages

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}

async def fetch_page(client: httpx.AsyncClient, url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        response = await client.get(url, follow_redirects=True, timeout=60.0)
        if response.status_code == 429:
            wait = 2 ** attempt  # 1s, 2s, 4s
            logging.warning(f"429 on {url}, retrying in {wait}s...")
            await asyncio.sleep(wait)
            continue
        response.raise_for_status()
        return response.text
    raise httpx.HTTPError(f"Failed after {retries} retries: {url}")


async def main():
    try:
        start = time.time()
        logging.info("Data Scraping Initialized")

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
            parse_arise_news,
        ]

        database = Database()

        async with httpx.AsyncClient(headers=HEADERS) as client:
            for idx, url in enumerate(data_url):
                logging.info(f"Scraping {url}")
                try:
                    content = await fetch_page(client, url)
                except httpx.HTTPError as e:
                    logging.warning(f"Failed to fetch {url}: {e}")
                    continue

                parsed = parsers[idx](content)
                data = preprocessor(parsed)
                articles = data.get("articles", [])

                source_name = list(source_id.keys())[idx]
                for article in articles:
                    article['source_id'] = source_id[source_name]
                    article['scraped_at'] = datetime.now().isoformat()

                if articles:
                    database.insert('parsed_articles', articles, conflict_column='hash')
                    database.update(
                        "sources",
                        source_id[source_name],
                        data.get('scraped_at', time.strftime("%Y-%m-%dT%H:%M:%S"))
                    )

        await scrape_recent_messages()
        end = time.time()
        logging.info(f"Time Taken: {end - start:.2f} seconds")
        return True

    except Exception as e:
        logging.error(f"An Error Occurred During Data Scraping: {e}")
