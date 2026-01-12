import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from scrapper.database import Database 

db = Database()

def get_all_events(limit=100):
    try:
        query = """
        SELECT
            e.id AS event_id,
            e.event_type,
            e.title AS event_title,
            e.location,
            e.state,
            e.severity,
            e.confidence,
            e.status,
            e.first_detected,
            e.last_updated,

            an.alert,
            an.escalation,
            an.brief,

            a.id AS article_id,
            a.title AS article_title,
            a.news_url,
            a.image_url,
            a.published_at,

            s.name AS source_name,
            s.credibility_score

        FROM events e
        LEFT JOIN analysis an ON an.event_id = e.id
        LEFT JOIN event_articles ea ON ea.event_id = e.id
        LEFT JOIN parsed_articles a ON a.id = ea.article_id
        LEFT JOIN sources s ON s.id = a.source_id

        ORDER BY e.last_updated DESC NULLS LAST, a.published_at DESC
        LIMIT %s
        """

        rows = db.fetch_all(query, (limit * 5,))

        events = {}

        for row in rows:
            eid = row["event_id"]

            if eid not in events:
                events[eid] = {
                    "event_id": eid,
                    "event_type": row["event_type"],
                    "title": row["event_title"],
                    "location": row["location"],
                    "state":row["state"],
                    "severity": row["severity"],
                    "confidence": row["confidence"],
                    "status": row["status"],
                    "first_detected": row["first_detected"],
                    "last_updated": row["last_updated"],
                    "analysis": {
                        "alert": row["alert"],
                        "escalation": row["escalation"],
                        "brief": row["brief"],
                    },
                    "image_urls": [],
                    "articles": [],
                }

            if row["article_id"]:
                events[eid]["articles"].append({
                    "article_id": row["article_id"],
                    "title": row["article_title"],
                    "url": row["news_url"],
                    "source": row["source_name"],
                    "credibility_score": row["credibility_score"],
                    "published_at": row["published_at"],
                })

                if row["image_url"] and len(events[eid]["image_urls"]) < 2:
                    events[eid]["image_urls"].append(row["image_url"])

        return list(events.values())[:limit]

    except Exception as e:
        logging.error(f"[get_all_events] Failed: {e}")
        return []

def search_events(event_type, location=None, limit=15):
    try:
        query = """
        SELECT
            e.id AS event_id,
            e.event_type,
            e.title AS event_title,
            e.location,
            e.state,
            e.severity,
            e.confidence,
            e.status,
            e.first_detected,
            e.last_updated,

            a.id AS article_id,
            a.title AS article_title,
            a.news_url,
            a.image_url,
            a.published_at,

            s.name AS source_name,
            s.credibility_score

        FROM events e
        LEFT JOIN event_articles ea ON ea.event_id = e.id
        LEFT JOIN parsed_articles a ON a.id = ea.article_id
        LEFT JOIN sources s ON s.id = a.source_id
        WHERE LOWER(e.event_type) = LOWER(%s)
        """

        params = [event_type]

        if location:
            query += " AND LOWER(e.state) = LOWER(%s)"
            params.append(location)

        query += """
        ORDER BY e.last_updated DESC NULLS LAST, a.published_at DESC
        LIMIT %s
        """

        params.append(limit * 5)

        rows = db.fetch_all(query, tuple(params))

        events = {}

        for row in rows:
            eid = row["event_id"]

            if eid not in events:
                events[eid] = {
                    "event_id": eid,
                    "event_type": row["event_type"],
                    "title": row["event_title"],
                    "location": row["location"],
                    "state": row["state"],
                    "severity": row["severity"],
                    "confidence": row["confidence"],
                    "status": row["status"],
                    "image_urls": [],
                    "articles": [],
                }

            if row["article_id"]:
                events[eid]["articles"].append({
                    "title": row["article_title"],
                    "url": row["news_url"],
                    "source": row["source_name"],
                    "credibility_score": row["credibility_score"],
                    "published_at": row["published_at"],
                })

                if row["image_url"] and len(events[eid]["image_urls"]) < 2:
                    events[eid]["image_urls"].append(row["image_url"])

        return list(events.values())[:limit]

    except Exception as e:
        logging.error(f"[search_events] Failed: {e}")
        return []


