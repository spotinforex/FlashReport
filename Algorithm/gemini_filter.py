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
            {
                "parts": [
                    {"text": prompt}
                ]
            }
        ]
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(
        GEMINI_URL,
        headers=headers,
        data=json.dumps(payload),
        timeout=180
    )

    response.raise_for_status()

    data = response.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]



