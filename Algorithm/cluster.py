import logging,sys
logger = logging.getLogger("runner")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

from datetime import timedelta, datetime
from collections import defaultdict
import json
from Algorithm.gemini_filter import call_gemini
from scrapper.database import Database
from pathlib import Path
import time

db = Database()

def create_event(db, event):
    """Creates a new event and returns its ID safely"""
    try:
        query = """
        INSERT INTO events (
            event_type,
            title,
            location,
            first_detected,
            last_updated,
            severity,
            confidence,
            state,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """

        row = db.fetch_one(query, (
            event.get("event_type"),
            event.get("summary"),
            event.get("location"),
            event.get("timestamp"),
            event.get("timestamp"),
            event.get("severity"),
            event.get("confidence"),
            event.get("state"),
            "new"
        ))

        if not row or "id" not in row:
            logging.error(f"Failed to create event, query returned: {row}")
            return None

        return row["id"]

    except Exception as e:
        logging.error(f"Error creating event: {e}")
        return None

def link_article_to_event(db, event_id, article_id, relevance):
    ''' Link many Articles of the same event together '''
    query = """
    INSERT INTO event_articles (
        event_id,
        article_id,
        relevance_score
    )
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """

    db.execute(query, (event_id, article_id, relevance))

def update_event(db, event_id, event):
    ''' Update Cluster based on new events '''
    query = """
    UPDATE events
    SET
        last_updated = %s,
        severity = %s,
        confidence = %s,
        title = CASE
            WHEN %s > confidence THEN %s
            ELSE title
        END,
        status = 'ongoing'
    WHERE id = %s;
    """

    db.execute(query, (
        event["timestamp"],
        event["severity"],
        event["confidence"],
        event["confidence"],
        event["summary"],
        event_id
    ))


TIME_WINDOW_DAYS = 30

def assign_cluster(data):
    ''' Creating and Assigning Clusters to Signals '''
    try: 
        event_candidate = {
                        'timestamp': data.get('created_at'),
                        'event_type': data.get('signal_type'),
                        'location': data.get('extracted_location'),
                        'article_id': data.get('article_id'),
                        'confidence': data.get('confidence'),
                        'severity': data.get('severity'),
                        'summary': data.get('summary'),
                        'state': data.get('state')
                    }
        query = """
        SELECT id, last_updated
        FROM events
        WHERE
            event_type = %s
            AND location = %s
            AND last_updated >= %s
            AND status != 'resolved'
        ORDER BY last_updated DESC
        LIMIT 1;
        """

        cutoff_time = event_candidate["timestamp"] - timedelta(days=TIME_WINDOW_DAYS)

        match = db.fetch_one(query, (
            event_candidate["event_type"],
            event_candidate["location"],
            cutoff_time
        ))
        
        if match:
            event_id = match.get('id')

            link_article_to_event(
                db,
                event_id,
                event_candidate.get("article_id"),
                event_candidate.get("confidence")
            )

            update_event(db,event_id, event_candidate)
            return event_id

        
        event_id = create_event(db, event_candidate)
        if event_id is None:
            logging.error(f"Could not create event for article {event_candidate.get('article_id')}")
            return None

        link_article_to_event(
            db,
            event_id,
            event_candidate["article_id"],
            event_candidate["confidence"]
        )

        return event_id
    except Exception as e:
        import traceback
        logging.error(f"An Error Occurred While Assigning Clusters: {e}\n{traceback.format_exc()}")

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def prepare_clusters_for_gemini():
    ''' Preprocess Cluster For Gemini '''
    query = ''' 
        WITH recent_events AS (
            SELECT
                e.id AS event_id,
                e.event_type,
                e.title,
                e.location,
                e.first_detected,
                e.last_updated,
                e.severity,
                e.confidence,
                e.status
            FROM events e
            WHERE e.last_updated >= NOW() - INTERVAL '4 HOURS'
            AND e.status != 'resolved'
        )
        SELECT
            re.event_id,
            re.event_type,
            re.title AS cluster_title,
            re.location AS cluster_location,
            re.first_detected,
            re.last_updated,
            re.severity AS cluster_severity,
            re.confidence AS cluster_confidence,
            re.status AS cluster_status,
            a.article_id,
            pa.title AS article_text,
            a.relevance_score
        FROM recent_events re
        LEFT JOIN event_articles a
            ON re.event_id = a.event_id
        LEFT JOIN parsed_articles pa
            ON a.article_id = pa.id
        ORDER BY re.last_updated DESC, a.relevance_score DESC;
        '''
    records = db.fetch_all(query)
    clusters = defaultdict(lambda: {"event_id": None, "event_type": None, "location": None, "articles": []})

    for row in records:
        event_id = row["event_id"]

        cluster = clusters[event_id]
        cluster["event_id"] = event_id
        cluster["event_type"] = row["event_type"]
        cluster["location"] = row["cluster_location"]
        cluster["last_updated"] = row["last_updated"]
        cluster["severity"] = row["cluster_severity"]
        cluster["confidence"] = row["cluster_confidence"]
        cluster["status"] = row["cluster_status"]

        cluster["articles"].append({
            "article_id": row["article_id"],
            "text": row["article_text"],
            "relevance": row["relevance_score"]
        })


    # convert defaultdict to normal list of dicts
    parsed_values = list(clusters.values())
    return parsed_values

def save_gemini_cluster_analysis(db, cluster_results):
    """
    Saves Gemini cluster-level analysis to the database.
    """
    try:
        analysis_list = []
        for result in cluster_results.get("results", []):
            query = """
            UPDATE events
            SET
                status = CASE WHEN %s THEN 'alert' ELSE status END,
                severity = CASE WHEN %s THEN 'high' ELSE severity END,
                last_updated = %s
            WHERE id = %s;
            """

            db.execute(query, (
                result.get("alert", False),
                result.get("escalation", False),
                datetime.utcnow(),
                result.get("event_id")
            ))
            gemini_analysis = (
                result.get("event_id"),
                result.get("same_incident"),
                result.get("escalation"),
                result.get("alert"),
                result.get("brief")
                )
            analysis_list.append(gemini_analysis)
        query2 = """
                INSERT INTO analysis (
                    event_id,
                    same_incident,
                    escalation,
                    alert,
                    brief
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (event_id)
                DO UPDATE SET
                    same_incident = EXCLUDED.same_incident,
                    escalation = EXCLUDED.escalation,
                    alert = EXCLUDED.alert,
                    brief = EXCLUDED.brief;
                """
        db.execute_batch(query2, analysis_list)
    except Exception as e:
        logging.error(f"An Error Occurred When Saving Gemini Analysis Cluster: {e}")
        return None

def safe_parse_gemini_response(response):
    ''' Response Checker '''
    if response is None:
        return None

    # If Gemini already returned a dict
    if isinstance(response, dict):
        return response

    # If Gemini returned text
    if isinstance(response, str):
        response = response.strip()

        if not response:
            return None

        # Remove markdown code fences
        if response.startswith("```"):
            response = response.strip("`")
            response = response.replace("json", "", 1).strip()

        try:
            return json.loads(response)
        except json.JSONDecodeError:
            logging.error(f"Gemini returned non-JSON:\n{response}")
            return None

    return None


def clustering_pipeline():
    """ Pipeline For Handling Clustering Process """
    try:
        start = time.time()
        cutoff_time = datetime.utcnow() - timedelta(days=5)
        query = "SELECT * FROM signals WHERE created_at >= %s"
        data_list = db.fetch_all(query, (cutoff_time,))
        logging.info("Assigning Clusters In Progress")
        for data in data_list:
            assign_cluster(data)

        logging.info("Preparing Cluster For Gemini")
        value = prepare_clusters_for_gemini()

        with open(Path.cwd() / "Algorithm/system_instructions/clustering_instructions.txt") as f:
            instructions = f.read()

        prompt = f"{instructions} REPORT: {json.dumps(value, default=str)}"

        logging.info("Calling Gemini")
        response = call_gemini(prompt)  # should return dict

        logging.info("Verifying Gemini Response")
        response = safe_parse_gemini_response(response)
        if response is None:
            return None
        logging.info("Response Retrieved, Saving Response")
        save_gemini_cluster_analysis(db, response)

        end = time.time()
        logging.info(f"Response Saved. Total Time Taken: {end - start:.2f} seconds")
        return True

    except Exception as e:
        import traceback
        logging.error(f"Failed To Cluster Headlines: {e}\n{traceback.format_exc()}")

