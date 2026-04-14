import os
import json
from datetime import datetime, timezone
import redis
from flask import Flask, request, jsonify

app = Flask(__name__)

# Connect to Redis using environment variables
r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"),
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

# Identify the instance (Cloud Run provides HOSTNAME automatically)
SERVER_ID = os.environ.get("HOSTNAME", "local")

@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json()
    if "message" not in data:
        return jsonify({"error": "Champ 'message' requis"}), 400

    # Enrich the data with server identity and timestamp
    entry = {
        "message": data["message"],
        "server_id": SERVER_ID,
        "published_at": datetime.now(timezone.utc).isoformat()
    }

    # Store in Redis with a 1-hour TTL (3600 seconds)
    key = f"event:{SERVER_ID}:{entry['published_at']}"
    r.setex(key, 3600, json.dumps(entry))
    
    return jsonify({"status": "published", "redis_key": key, "data": entry})

@app.route("/data")
def data():
    # SCAN finds keys without blocking the Redis server
    result = {}
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="event:*", count=100)
        for key in keys:
            value = r.get(key)
            ttl = r.ttl(key)
            if value:
                result[key] = {"data": json.loads(value), "ttl_remaining_seconds": ttl}
        if cursor == 0:
            break
            
    return jsonify({"server_id": SERVER_ID, "count": len(result), "entries": result})

@app.route("/health")
def health():
    try:
        r.ping()
        return jsonify({"status": "healthy", "server_id": SERVER_ID, "redis": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 503

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))