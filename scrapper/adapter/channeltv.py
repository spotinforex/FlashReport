import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from bs4 import BeautifulSoup
from datetime import datetime 
from pathlib import Path 
import time 

def parse_channel_news(html_content):
    """
    Parse Channel News TV website content and extract articles
    Args:
        html_content: Raw Html Data 
    Returns:
        parsed_data: Parsed Data in Json Format 
    """
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, 'lxml')

        parsed_data = {
            'source': 'Channel TV News',
            'source_url': 'https://www.channelstv.com/',
            'scraped_at': datetime.now().isoformat(),
            'articles': []
        }

        # Parse Latest News section
        logging.info("Parsing Channel News TV Latest News Section In Progress")
        main_article = soup.select_one("div.main__article-articles article")

        if main_article:
            title = main_article.select_one("h3 a").get_text(strip=True)
            link = main_article.select_one("h3 a")["href"]

            summary_tag = main_article.select_one(".post-excerpt p")
            summary = summary_tag.get_text(strip=True) if summary_tag else ""

            print("MAIN:")
            print(title, link, summary)
    except Exception as e:
        print(f"error: {e}")


