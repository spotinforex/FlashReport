import hmac, hashlib, os
from fastapi import Request, HTTPException

SECRET = os.getenv("BACKEND_SECRET")  

async def verify_request(request: Request):
    timestamp = request.headers.get("X-Timestamp")
    signature = request.headers.get("X-Signature")

    if not timestamp or not signature:
        raise HTTPException(401, "Unauthorized")

    # prevent replay attacks (allow 2 min drift)
    if abs(int(timestamp) - int(time.time())) > 120:
        raise HTTPException(401, "Request expired")

    expected_sig = hmac.new(
        SECRET.encode(),
        timestamp.encode(),
        hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected_sig, signature):
        raise HTTPException(403, "Forbidden")

