import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

import re, json
from Algorithm.gemini_filter import call_gemini
from scrapper.database import Database 
from pathlib import Path
from datetime import datetime

def normalize_text(text):
    """
    Normalizes messages:
    - lowercase
    - strip leading/trailing spaces
    - remove multiple spaces
    - remove newlines
    """
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)  # collapse multiple spaces
    text = text.replace('\n', ' ')
    return text

def contains_risk_keyword(text):
    """
    Returns list of matched keywords in text, or None if none.
    """
    RISK_KEYWORDS = {
    "kidnapping": ["kidnap","kidnapping", "abduction", "hostage","hostages", "missing person"],
    "armed attack": ["gunmen","terrorists","terrorist","terror","bandits","bandit", "militants","militant", "attackers", "robbery"],
    "protest": ["protest","demonstration", "strike", "rally", "march", "unrest"],
    "flood": ["flood", "inundation", "waterlogged", "overflow"],
    "fire": ["fire","fire outbreak", "electrical fire", "building fire", "explosion"],
    "riot/violence": ["shooting", "clash", "assault", "massacre", "riot"]
    }
    for category, keywords in RISK_KEYWORDS.items():
        if any(k in text for k in keywords):
            return category
    return None 

def ingest_message(msg):
    """
    Runs on every incoming news/telegram message.
    Cheap, fast, deterministic.
    """
    try:
        if not msg:
            return None

        text = normalize_text(msg)

        # hard filters
        if len(text) < 30:
            return None

        # Keyword Filtering 
        category = contains_risk_keyword(text)
        if category is None :
            return None

        return category
    except Exception as e:
        logging.error(f" An Error Occurred When Filtering Messages: {e}")

def gemini_results_to_signals(gemini_response: dict) -> list[dict]:
    """
    Convert Gemini batch output into DB-ready signal records
    """
    try:
        signals = []
        now = datetime.utcnow()
        gemini_response = json.loads(gemini_response)
        for item in gemini_response.get("results", []):
            if not item.get("is_real_incident"):
                continue  # hard filter: no noise in signals table

            location = item.get("location") or {}

            extracted_location = ", ".join(
                part for part in [
                    location.get("city"),
                    location.get("region"),
                    location.get("country"),
                ]
                if part
            ) or None

            signal = {
                "article_id": item["id"],                  
                "signal_type": item.get("event_type"),
                "confidence": item.get("confidence"),
                "extracted_location": extracted_location,
                "created_at": now,
                "severity": item.get("severity"),
                "is_ongoing": item.get("is_ongoing"),
                "summary": item.get("summary"),
            }

            signals.append(signal)
        database = Database()
        status = database.insert('signals', signals)
        if status is False:
            logging.info("Failed To Insert Signals To Database")
            return False
        return True
    except Exception as e:
        logging.error(f" An Error Occurred When Converting Gemini Response to Signals: {e}")
        return False

def filter_pipeline():
    '''
    Pipeline For Filtering Messages 
    '''
    try:
        logging.info("Filtering Processing Initialized")
        database = Database()
        query = """
            SELECT *
            FROM parsed_articles
            WHERE scraped_at >= NOW() - INTERVAL '72 hours'
            ORDER BY scraped_at DESC;
            """
        rows = database.fetch_all(query)
        results = []

        for row in rows:
            id = row[0]
            msg = row[2]
            result = ingest_message(msg)

            if result:
                results.append({'id' : id,
                                'headline': msg})
        logging.info("Keyword Filtering Completed, Calling Gemini in Progress")
        path = Path.cwd()
        with open(f"{path}/Algorithm/system_instructions/filter_instructions.txt", "r") as w:
            instructions = w.read()
        prompt = f"{instructions} {results}"
        response = call_gemini(prompt)
        logging.info("Retrieved Gemini Response, Inserting to Database")
        status = gemini_results_to_signals(response)
        return status
    except Exception as e:
        logging.error(f"An Error Occurred During The Filtering Pipeline. {e}")



            
        
