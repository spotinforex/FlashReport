import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

import re

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



            
        
