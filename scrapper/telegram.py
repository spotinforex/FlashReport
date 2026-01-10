import os
from telethon import TelegramClient
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import hashlib
import logging
import re
from scrapper.database import Database 
import time

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")

CHANNELS = {
    "Gist Ng": {"id": "Gist_Ng"},
    "Naija News": {"id": "naijanews"},
    "Legit.ng News": {"id": "legitng"},
    "Naija Amebo Gist Nigeria": {"id": "naija_amebo"},
    "Nigeria News": {"id": "nigerianws"},
}

# Map Telegram channels to source_id in your database
telegram_id = {
    'Gist Ng': '0416f8e7-162b-49e7-8c82-bbbe2ada058f',
    'Naija Amebo Gist Nigeria': '0d02f9b7-4282-40d4-87e5-e86c08ec911d',
    'Legit.ng News': '2f00af2d-bf6f-4677-aa27-d991ed0ff887',
    'Naija News': '984cd207-8167-45c3-8abd-9bf1fa05baec',
    'Nigeria News': '9dc82a1a-dceb-47f1-8807-6c4703bbc46c',
}

TIME_CUTOFF = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=3)

client = TelegramClient("flashreport_session", api_id, api_hash)

seen_hashes = set()

# Extract first URL from text
URL_PATTERN = re.compile(r"https?://\S+")

def normalize_text(text):
    if not text:
        return None
    return " ".join(text.splitlines()).strip()

def message_hash(channel_id, text, published_at):
    combined = f"{channel_id}{text}{published_at.isoformat()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def extract_url(text):
    match = URL_PATTERN.search(text)
    return match.group(0) if match else None

database = Database()  

async def scrape_recent_messages():
    try:
        start = time.time()
        await client.start()
        logging.info(f"Scraping messages since {TIME_CUTOFF.isoformat()}")

        parsed_articles = []

        for channel_name, channel in CHANNELS.items():
            source_uuid = telegram_id.get(channel_name)
            last_scraped_time = datetime.utcnow().isoformat()

            async for message in client.iter_messages(channel["id"]):
                if message.date < TIME_CUTOFF:
                    break
                if not message.text:
                    continue

                text = normalize_text(message.text)
                h = message_hash(channel["id"], text, message.date)

                if h in seen_hashes:
                    continue
                seen_hashes.add(h)

                title = text[:150] if text else None
                news_url = extract_url(text)
                image_url = None  # can add media later
                published_at = message.date
                scraped_at = datetime.utcnow()

                parsed_articles.append({
                    "source_id": source_uuid,
                    "title": title,
                    "news_url": news_url,
                    "image_url": image_url,
                    "published_at": published_at,
                    "scraped_at": scraped_at,
                    "hash": h
                })

            # Update the sources table for this channel after scraping it
            database.update(
                'sources',
                source_uuid,
                last_scraped_time
            )

        logging.info(f"Prepared {len(parsed_articles)} parsed_articles entries")
        await client.disconnect()

        # Insert all scraped articles
        database.insert('parsed_articles', parsed_articles,conflict_column = 'hash')
        end = time.time()
        logging.info(f" Telegram Messages Retrived Successfully. Time Taken: {end-start:.2f} seconds")
    except Exception as e:
        logging.error(f" An Error Occurred When Retrieving Telegram Messages {e}")
        return False

    

