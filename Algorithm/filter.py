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

STATE_KEYWORDS = {
    "Abia": ["abia", "abiastate", "abia state", "umuahia", "aba", "aba city"],
    "Adamawa": ["adamawa", "adamawastate", "adamawa state", "yola", "jimeta"],
    "Akwa Ibom": ["akwa ibom", "akwa-ibom", "akwaibom", "akwa ibom state", "akwaibomstate", "uyo"],
    "Anambra": ["anambra", "anambrastate", "anambra state", "awka", "onitsha", "nnewi"],
    "Bauchi": ["bauchi", "bauchistate", "bauchi state", "azare"],
    "Bayelsa": ["bayelsa", "bayelsastate", "bayelsa state", "yenagoa"],
    "Benue": ["benue", "benuestate", "benue state", "makurdi", "gboko", "otukpo"],
    "Borno": ["borno", "bornostate", "borno state", "maiduguri", "bama"],
    "Cross River": ["cross river", "cross-river", "crossriver", "cross river state", "crossriverstate", "calabar", "ogoja"],
    "Delta": ["delta", "deltastate", "delta state", "asaba", "warri", "sapele", "ughelli"],
    "Ebonyi": ["ebonyi", "ebonyistate", "ebonyi state", "abakaliki"],
    "Edo": ["edo", "edostate", "edo state", "benin city", "benin-city", "benin", "auchi"],
    "Ekiti": ["ekiti", "ekitistate", "ekiti state", "ado ekiti", "ado-ekiti", "ado"],
    "Enugu": ["enugu", "enugustate", "enugu state", "nsukka", "enugu-ezike", "enugu city"],
    "FCT": ["fct", "abuja", "fcta", "fct abuja", "abujafct", "gwagwalada", "kuje", "bwari", "federal capital territory"],
    "Gombe": ["gombe", "gombestate", "gombe state", "deba", "kaltungo"],
    "Imo": ["imo", "imostate", "imo state", "owerri", "orlu"],
    "Jigawa": ["jigawa", "jigawastate", "jigawa state", "dutse", "hadejia", "gumel"],
    "Kaduna": ["kaduna", "kadunastate", "kaduna state", "zaria", "kafanchan", "kaduna city"],
    "Kano": ["kano", "kanostate", "kano state", "kano city"],
    "Katsina": ["katsina", "katsinastate", "katsina state", "daura", "funtua"],
    "Kebbi": ["kebbi", "kebbistate", "kebbi state", "birnin kebbi", "argungu"],
    "Kogi": ["kogi", "kogistate", "kogi state", "lokoja", "okene", "kabba"],
    "Kwara": ["kwara", "kwarastate", "kwara state", "ilorin", "offa"],
    "Lagos": ["lagos", "lagosstate", "lagos state", "ikeja", "lekki", "ikorodu", "epe", "badagry", "lagos island", "lagos mainland"],
    "Nasarawa": ["nasarawa", "nasarawastate", "nasarawa state", "lafia", "keffi", "akwanga"],
    "Niger": ["niger state", "nigerstate", "niger", "minna", "bida", "suleja"],
    "Ogun": ["ogun", "ogunstate", "ogun state", "abeokuta", "ijebu ode", "ijebu-ode", "sagamu"],
    "Ondo": ["ondo", "ondostate", "ondo state", "akure", "ondo city"],
    "Osun": ["osun", "osunstate", "osun state", "osogbo", "ile-ife", "ilesha", "ilesa"],
    "Oyo": ["oyo", "oyostate", "oyo state", "ibadan", "ogbomoso", "oyo town"],
    "Plateau": ["plateau", "plateaustate", "plateau state", "jos", "bukuru", "jos city"],
    "Rivers": ["rivers", "riversstate", "rivers state", "port harcourt", "portharcourt", "ph", "phc"],
    "Sokoto": ["sokoto", "sokotostate", "sokoto state", "wurno"],
    "Taraba": ["taraba", "tarabastate", "taraba state", "jalingo", "wukari"],
    "Yobe": ["yobe", "yobestate", "yobe state", "damaturu", "potiskum"],
    "Zamfara": ["zamfara", "zamfarastate", "zamfara state", "gusau", "kaura namoda"]
}

def extract_state_from_location(location_string):
    """
    Extract Nigerian state from a location string using keyword matching.
    """
    if not location_string:
        logging.warning("No Location to be filtered is provided")
        return None
    
    # Remove spaces and convert to lowercase for compound matching
    location_lower = location_string.lower()
    location_nospace = location_string.replace(" ", "").lower()
    
    # Check each state's keywords
    for state, keywords in STATE_KEYWORDS.items():
        for keyword in keywords:
            # Check both with and without spaces
            if keyword in location_lower or keyword in location_nospace:
                return state
    
    return None


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

            state = extract_state_from_location(extracted_location)

            signal = {
                "article_id": item["id"],                  
                "signal_type": item.get("event_type"),
                "confidence": item.get("confidence"),
                "extracted_location": extracted_location,
                "created_at": now,
                "severity": item.get("severity"),
                "is_ongoing": item.get("is_ongoing"),
                "summary": item.get("summary"),
                "state": state
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
            WHERE scraped_at >= NOW() - INTERVAL '4 hours'
            ORDER BY scraped_at DESC;
            """
        rows = database.fetch_all(query)
        results = []

        for row in rows:
            id = row.get('id')
            msg = row.get('title')
            result = ingest_message(msg)

            if result:
                results.append({'id' : id,
                                'headline': msg})
        logging.info(f"Keyword Filtering Completed, Calling Gemini in Progress. {len(results)} keywords filtered.")
        if len(results) == 0:
            return None
        path = Path.cwd()
        with open(f"{path}/Algorithm/system_instructions/filter_instructions.txt", "r") as w:
            instructions = w.read()
        prompt = f"{instructions} {results}"
        response = call_gemini(prompt)
        logging.info("Retrieved Gemini Response, Inserting to Database")
        status = gemini_results_to_signals(response)
        return status
    except Exception as e:
        import traceback
        logging.error(f"An Error Occurred During The Filtering Pipeline. {e}\n{traceback.format_exc()}")


