import logging, sys, time, re
from bs4 import BeautifulSoup
from datetime import datetime

logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def parse_saharareporters_news(html_content):
    try:
        start = time.time()
        soup = BeautifulSoup(html_content, "lxml")

        parsed_data = {
            "source": "Sahara Reporters",
            "source_url": "https://saharareporters.com",
            "scraped_at": datetime.now().isoformat(),
            "articles": [],
        }

        logging.info("Parsing Sahara Reporters Articles In Progress")

        BASE_URL = "https://saharareporters.com"
        articles = soup.find_all("div", class_="node--type-article")

        for article in articles:
            data = {}

            # ---------------- Title & URL ----------------
            title_elem = article.find("h2", class_="title")
            if title_elem:
                link = title_elem.find("a")
                if link:
                    raw_url = link.get("href", "").strip()
                    data["url"] = (
                        BASE_URL + raw_url if raw_url.startswith("/") else raw_url
                    )
                    data["title"] = link.get_text(strip=True)

            # ---------------- Image ----------------
            img_elem = article.find("img", property="schema:image")
            if img_elem:
                src = img_elem.get("src", "")
                data["image_url"] = (
                    BASE_URL + src if src.startswith("/") else src
                )

            # ---------------- Date ----------------
            date_text = article.get_text(" ", strip=True)

            match = re.search(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                date_text
            )
            if match:
                data["date"] = match.group(0)

            if data.get("title"):
                parsed_data["articles"].append(data)

        logging.info("Parsing Sahara Reporters Articles Completed")

        # -------- Remove duplicates --------
        logging.info("Removing duplicate articles")
        seen = set()
        unique_articles = []

        for article in parsed_data["articles"]:
            if article["title"] not in seen:
                seen.add(article["title"])
                unique_articles.append(article)

        parsed_data["articles"] = unique_articles
        parsed_data["total_articles"] = len(unique_articles)

        end = time.time()
        logging.info(
            f"Parsing Completed. Total Articles: {len(unique_articles)}. "
            f"Time Taken: {end - start:.2f} seconds."
        )

        return parsed_data

    except Exception as e:
        logging.error(f"An Error Occurred While Parsing Sahara Reporters: {e}")
        return None
