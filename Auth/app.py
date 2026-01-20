from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from Auth.db_communicator import get_all_events, search_events
from main import pipeline
from Auth.verifier import verify_request

app = FastAPI(
    title="FlashReport API",
    description="API interface for FlashReport event detection and retrieval",
    version="1.0.0"
)

origins = [
    "https://your-vercel-domain.vercel.app",  
    "http://localhost:3000",  
    "http://localhost:5173",                
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,        
    allow_credentials=True,
    allow_methods=["GET", "POST"], 
    allow_headers=["*"],           
)

@app.get("/")
async def health_check():
    return {"status": "ok", "service": "FlashReport"}

@app.post("/run-pipeline")
async def run_pipeline():
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
async def fetch_events(request: Request):
    try:
        await verify_request(request)
        events = get_all_events()
        return {"count": len(events), "events": events}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/search")
async def search_event(
    request: Request,
    keyword: str = Query(..., min_length=2, description="Event keyword"),
    location: str | None = Query(None, description="Optional location filter")
):
    await verify_request(request)
    try:
        results = search_events(event_type=keyword, location=location)
        return {"keyword": keyword, "location": location, "count": len(results), "events": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

