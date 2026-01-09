from flood_data import data 
import requests
from datetime import datetime, timedelta

import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

source_id = {'Weather Api':'dbff2799-51e0-44f8-b741-b6371b8e9c3c'}

def weather_api(lat, lon, days):
    try:
        logging.info("Retrieving Weather Data In Progress")

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=days)

        url = (
            "https://archive-api.open-meteo.com/v1/archive?"
            f"latitude={lat}&longitude={lon}"
            f"&start_date={start_date}&end_date={end_date}"
            "&daily=precipitation_sum&timezone=UTC"
        )

        response = requests.get(url, timeout=15).json()

        precip = response.get("daily", {}).get("precipitation_sum", [])
        if not precip:
            return False, "No precipitation data available"

        # ---- 30 DAY CHECK (highest confidence) ----
        if days >= 30:
            sum_30 = sum(precip[-30:])
            sum_7 = sum(precip[-7:])

            if sum_30 >= 300 and sum_7 >= 120:
                return True, (
                    f"Severe rainfall accumulation detected. "
                    f"30-day sum: {sum_30:.1f} mm, "
                    f"7-day sum: {sum_7:.1f} mm"
                )

        # ---- 7 DAY CHECK ----
        if days >= 7:
            sum_7 = sum(precip[-7:])
            if sum_7 >= 120:
                return True, f"Heavy rainfall spike in last 7 days: {sum_7:.1f} mm"

        # ---- 3 DAY CHECK ----
        if days >= 3:
            sum_3 = sum(precip[-3:])
            if sum_3 >= 80:
                return True, f"Intense rainfall spike in last 3 days: {sum_3:.1f} mm"

        return False, "No flood-relevant rainfall detected"

    except Exception as e:
        logging.error(f"Error calling Weather API: {e}")
        return "Weather verification failed"

