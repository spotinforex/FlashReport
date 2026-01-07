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

def create_event(db,event):
    ''' Creates New Events '''
    query = """
    INSERT INTO events (
        event_type,
        title,
        location,
        first_detected,
        last_updated,
        severity,
        confidence,
        status
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """

    return db.fetch_one(query, (
        event["event_type"],
        event["title"],
        event["location"],
        event["timestamp"],
        event["timestamp"],
        event["severity"],
        event["confidence"],
        "new"
    ))[0]

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
        status = 'ongoing'
    WHERE id = %s;
    """

    db.execute(query, (
        event["timestamp"],
        event["severity"],
        event["confidence"],
        event_id
    ))


TIME_WINDOW_DAYS = 30

def assign_cluster(data):
    ''' Creating and Assigning Clusters to Signals '''
    try:
        db = Database()

        event_candidate = {
                        'timestamp': datetime.utcnow(),
                        'event_type': data[2],
                        'location': data[4],
                        'article_id': data[1],
                        'confidence': data[3] ,
                        'severity': data[-3],
                        'title': data[-1]
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
            event_id = match[0]

            link_article_to_event(
                db,
                event_id,
                event_candidate["article_id"],
                event_candidate["confidence"]
            )

            update_event(db,event_id, event_candidate)
            return event_id

        
        event_id = create_event(db,event_candidate)

        link_article_to_event(
            db,
            event_id,
            event_candidate["article_id"],
            event_candidate["confidence"]
        )

        return event_id
    except Exception as e:
        logging.error(f" An Error Occurred While Assigning Clusters: {e}")

def convert_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def prepare_clusters_for_gemini():
    ''' Preprocess Cluster For Gemini '''

    db = Database()
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
            WHERE e.last_updated >= NOW() - INTERVAL '3 HOURS'
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
        (
            event_id, event_type, title, location, first_detected, last_updated,
            severity, confidence, status, article_id, article_text, relevance
        ) = row

        cluster = clusters[event_id]
        cluster["event_id"] = event_id
        cluster["event_type"] = event_type
        cluster["location"] = location
        cluster["last_updated"] = last_updated
        cluster["severity"] = severity
        cluster["confidence"] = confidence
        cluster["status"] = status

        cluster["articles"].append({
            "article_id": article_id,
            "text": article_text,
            "relevance": relevance
        })

    # convert defaultdict to normal list of dicts
    parsed_values = list(clusters.values())
    json_values = json.dumps(parsed_values, indent =2, default = convert_datetime)
    return json_values

def save_gemini_cluster_analysis(db, cluster_results):
    """
    Saves Gemini cluster-level analysis to the database.
    """
    try:
        analysis_list = []
        cluster_results = json.loads(cluster_results)
        for result in cluster_results.get("results", []):
            query = """
            UPDATE events
            SET
                status = CASE WHEN %s THEN 'alert' ELSE status END,
                severity = CASE WHEN %s THEN 'high' ELSE severity END,
                last_updated = %s,
            WHERE id = %s;
            """

            db.execute(query, (
                result.get("alert", False),
                result.get("escalation", False),
                datetime.utcnow(),
                result.get("event_id")
            ))
            gemini_analysis = { 
                'event_id': result.get("event_id"),
                'same_incident': result.get("same_incident"),
                'escalation': result.get("escalation"),
                'alert': result.get("alert"),
                'brief': result.get("brief")
                }
            analysis_list.append(gemini_analysis)
        db.insert('analysis', analysis_list)
    except Exception as e:
        logging.error(f"An Error Occurred When Saving Gemini Analysis Cluster: {e}")
        return None

def clustering_pipeline():
    """ Pipeline For Handling Clustering Process """
    try:
        start = time.time()
        db = Database()

        cutoff_time = datetime.utcnow() - timedelta(hours=3)
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

        logging.info("Response Retrieved, Saving Response")
        save_gemini_cluster_analysis(db, response)

        end = time.time()
        logging.info(f"Response Saved. Total Time Taken: {end - start:.2f} seconds")

    except Exception as e:
        import traceback
        logging.error(f"Failed To Cluster Headlines: {e}\n{traceback.format_exc()}")



if __name__ == "__main__":
    clustering_pipeline()
    
            
    



                    
            
    
    

