import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

import requests
import json,time,os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "models/gemini-3-flash-preview"

GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/"
    f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
)

def call_gemini(prompt: str) -> str:
    payload = {
        "contents": [
            {"parts": [{"text": prompt}]}
        ]
    }

    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(GEMINI_URL, headers=headers, data=json.dumps(payload), timeout=180)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to call Gemini API: {e}") from e

    data = response.json()

    # Defensive parsing
    candidates = data.get("candidates")
    if not candidates or not isinstance(candidates, list):
        raise ValueError("No candidates returned from Gemini")

    content = candidates[0].get("content")
    if not content or not isinstance(content, dict):
        raise ValueError("No content in Gemini candidate")

    parts = content.get("parts")
    if not parts or not isinstance(parts, list):
        raise ValueError("No parts in Gemini content")

    text = parts[0].get("text")
    if text is None:
        raise ValueError("No text in Gemini part")

    return text




