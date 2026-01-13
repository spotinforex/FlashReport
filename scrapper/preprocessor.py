from datetime import datetime, timedelta
from dateutil import parser
import re
import hashlib

import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def parse_relative_time(text: str, reference_time: datetime):
    """
    Parse relative times like:
    - '4 hrs ago'
    - '15 mins ago'
    - '1 day ago'
    - 'yesterday'
    """
    try:
        if not text:
            logging.warning("No Data Found")
            return None

        text = text.lower().strip()

        if text == "yesterday":
            return reference_time - timedelta(days=1)

        match = re.search(r"(\d+)\s*(min|mins|minute|minutes|hr|hrs|hour|hours|day|days)", text)
        if not match:
            return None

        value = int(match.group(1))
        unit = match.group(2)

        if unit.startswith("min"):
            return reference_time - timedelta(minutes=value)
        if unit.startswith("hr") or unit.startswith("hour"):
            return reference_time - timedelta(hours=value)
        if unit.startswith("day"):
            return reference_time - timedelta(days=value)

        return None
    except Exception as e:
        logging.error(f"Error Parsing Relative Time. {e}")
        return None


def extract_date_from_string(text: str):
    """
    Extract YYYY/MM or YYYY/MM/DD from URL or image paths.
    """
    try:
        if not text:
            return None

        match = re.search(r"(20\d{2})[/-](\d{2})(?:[/-](\d{2}))?", text)
        if not match:
            return None

        year, month, day = match.group(1), match.group(2), match.group(3) or "01"
        
        return datetime(int(year), int(month), int(day))
    except ValueError:
        return None


def compute_article_hash(title: str, url: str):
    """
    Compute a SHA-256 hash of title + url for deduplication
    """
    try:
        m = hashlib.sha256()
        combined = (title + url).encode("utf-8")
        m.update(combined)
        return m.hexdigest()
    except Exception as e:
        logging.error(f"Failed To Hash. Error {e}")
        return None


def preprocessor(data: dict):
    """
    Preprocess and normalize articles with:
    - published_at timestamp
    - published_at_source
    - article hash
    """
    try:
        logging.info("Preprocessing Step Initialized")
        
        if data is None:
            logging.warning("No data Provided for Preprocessing")
            return None
            
        scraped_at = parser.isoparse(data["scraped_at"])
        articles = data.get("articles", [])

        for article in articles:
            published_at = None
            source = None

            # Relative time first
            if "date_posted" in article:
                published_at = parse_relative_time(article["date_posted"], scraped_at)
                if published_at:
                    source = "date_posted_relative"

            # Absolute date
            if not published_at and article.get("date_posted"):
                try:
                    published_at = parser.parse(article["date_posted"])
                    source = "date_posted_absolute"
                except Exception:
                    pass

            # Infer from image URL
            if not published_at:
                published_at = extract_date_from_string(article.get("image_url"))
                if published_at:
                    source = "image_url"

            # Infer from article URL
            if not published_at:
                published_at = extract_date_from_string(article.get("url"))
                if published_at:
                    source = "url"

            # Fallback
            if not published_at:
                published_at = scraped_at
                source = "scraped_at_fallback"

            article["published_at"] = published_at.isoformat()

            # Compute unique hash
            article["hash"] = compute_article_hash(article.get("title", ""), article.get("url", ""))
        logging.info("Preprocessing Step Completed")
        return data
    except Exception as e:
        logging.error(f"Failed To Preprocess Data")
        return None 
    
