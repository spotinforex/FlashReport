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
from config import TIME_WINDOW_DAYS, CLUSTER_TIME
from pathlib import Path
import time
from itertools import islice
from config import batch_size
from collections import defaultdict

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
        severity = CASE
            WHEN %s = 'high' AND severity <> 'high' THEN 'high'
            WHEN %s = 'medium' AND severity = 'low' THEN 'medium'
            ELSE severity
        END,
        confidence = GREATEST(confidence, %s),
        title = CASE
            WHEN %s > confidence THEN %s
            ELSE title
        END,
        status = 'ongoing'
    WHERE id = %s;
    """

    db.execute(query, (
    event["timestamp"],           # last_updated
    event["severity"],            # check if new severity is 'high'
    'medium',                      # escalate low -> medium if needed
    event["confidence"],          # for GREATEST comparison
    event["confidence"],          # compare with current confidence for title
    event["summary"],             # new title if confidence is higher
    event_id                      # WHERE id
    ))


def validate_event_candidate(event_candidate):
    """Validate required fields in event candidate"""
    required_fields = ['timestamp', 'event_type', 'state', 'article_id']
    missing = [field for field in required_fields if not event_candidate.get(field)]
    
    if missing:
        raise ValueError(f"Missing required fields: {missing}")
    
    return True

def assign_cluster(db, data):
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
        # Validate required fields
        try:
            validate_event_candidate(event_candidate)
        except ValueError as e:
            logging.error(f"Invalid event candidate: {e}")
            return None
            
        query = """
        SELECT id, last_updated
        FROM events
        WHERE
            event_type = %s
            AND state = %s
            AND location = %s
            AND last_updated >= %s
        ORDER BY
            last_updated DESC
        LIMIT 1;
        """

        cutoff_time = event_candidate["timestamp"] - timedelta(days=TIME_WINDOW_DAYS)

        match = db.fetch_one(query, (
            event_candidate["event_type"],
            event_candidate["state"],
            event_candidate["location"],
            cutoff_time
        ))
        
        if match:
            event_id = match.get('id')
            with db.transaction():
                link_article_to_event(
                    db,
                    event_id,
                    event_candidate.get("article_id"),
                    event_candidate.get("confidence")
                )

                update_event(db,event_id, event_candidate)
            return event_id

        with db.transaction():
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
    """
    Prepare ONLY un-analyzed clusters for Gemini.
    Idempotent and safe to run repeatedly.
    """

    query = '''
    WITH unanalyzed_events AS (
        SELECT
            e.id AS event_id,
            e.event_type,
            e.title,
            e.location,
            e.state,
            e.first_detected,
            e.last_updated,
            e.severity,
            e.confidence,
            e.status
        FROM events e
        WHERE
            e.status != 'resolved'
            AND NOT EXISTS (
                SELECT 1
                FROM analysis a
                WHERE a.event_id = e.id
            )
    )
    SELECT
        ue.event_id,
        ue.event_type,
        ue.title AS cluster_title,
        ue.location AS cluster_location,
        ue.first_detected,
        ue.last_updated,
        ue.severity AS cluster_severity,
        ue.confidence AS cluster_confidence,
        ue.status AS cluster_status,
        ea.article_id,
        pa.title AS article_text,
        ea.relevance_score
    FROM unanalyzed_events ue
    LEFT JOIN event_articles ea
        ON ue.event_id = ea.event_id
    LEFT JOIN parsed_articles pa
        ON ea.article_id = pa.id
    ORDER BY
        ue.last_updated ASC,
        ea.relevance_score DESC;
    '''

    records = db.fetch_all(query)

    if not records:
        return []

    clusters = defaultdict(lambda: {
        "event_id": None,
        "event_type": None,
        "location": None,
        "last_updated": None,
        "severity": None,
        "confidence": None,
        "status": None,
        "articles": []
    })

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

        if row["article_id"]:
            cluster["articles"].append({
                "article_id": row["article_id"],
                "text": row["article_text"],
                "relevance": row["relevance_score"]
            })

    return list(clusters.values())


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

def chunked_iterable(iterable, size):
    """ Breaks Clusters Into Batches"""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def clustering_pipeline():
    """Pipeline for handling clustering process"""
    try:
        start = time.time()
        cutoff_time = datetime.utcnow() - timedelta(hours=CLUSTER_TIME)
        
        # Fetch signals
        query = "SELECT * FROM signals WHERE created_at >= %s ORDER BY created_at ASC"
        data_list = db.fetch_all(query, (cutoff_time,))
        logging.info(f"Processing {len(data_list)} signals")
        
        # Track metrics
        metrics = {'total_signals': len(data_list), 'new_events': 0, 'merged_events': 0, 'failed': 0}
        
        # Assign clusters
        logging.info("Assigning clusters in progress")
        for data in data_list:
            event_id = assign_cluster(db, data)
            if event_id:
                result = db.fetch_one("SELECT COUNT(*) as count FROM event_articles WHERE event_id = %s", (event_id,))
                if result and result['count'] == 1:
                    metrics['new_events'] += 1
                else:
                    metrics['merged_events'] += 1
            else:
                metrics['failed'] += 1
        
        logging.info(f"Clustering complete: {metrics}")
        
        # Prepare clusters
        logging.info("Preparing clusters for Gemini")
        clusters = prepare_clusters_for_gemini()
        if not clusters:
            logging.warning("No clusters to analyze")
            return True
        
        with open(Path.cwd() / "Algorithm/system_instructions/clustering_instructions.txt") as f:
            instructions = f.read()
        
        logging.info(f"Sending clusters to Gemini in batches of {batch_size}. Total Clusters: {len(clusters)}")
        for batch in chunked_iterable(clusters, batch_size):
            prompt = f"{instructions} REPORT: {json.dumps(batch, default=str)}"
            response = call_gemini(prompt)
            response = safe_parse_gemini_response(response)
            if response:
                save_gemini_cluster_analysis(db, response)
        
        end = time.time()
        logging.info(f"Pipeline complete. Time: {end - start:.2f}seconds.")
        return True
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        return False
