from fastapi import FastAPI, Query, HTTPException
from Auth.db_communicator import get_all_events, search_events
from main import pipeline

app = FastAPI(
    title="FlashReport API",
    description="API interface for FlashReport event detection and retrieval",
    version="1.0.0"
)

@app.get("/")
async def health_check():
    return {"status": "ok", "service": "FlashReport"}


@app.post("/run-pipeline")
async def run_pipeline():
    """
    Triggers the FlashReport ingestion + clustering + Gemini analysis pipeline
    """
    try:
        result = await pipeline()
        return {
            "status": "success",
            "message": "Pipeline executed successfully",
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events")
async def fetch_events():
    """
    Fetch all detected events
    """
    try:
        events = get_all_events()
        return {
            "count": len(events),
            "events": events
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/search")
async def search_event(
    keyword: str = Query(..., min_length=2, description="Event keyword"),
    location: str | None = Query(None, description="Optional location filter")
):
    """
    Search events by keyword (required) and location (optional)
    """
    try:
        results = search_events(event_type=keyword, location=location)
        return {
            "keyword": keyword,
            "location": location,
            "count": len(results),
            "events": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


