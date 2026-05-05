import os, json, redis, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from google.cloud import tasks_v2, storage, firestore

app = Flask(__name__)
db = firestore.Client()
r = redis.Redis(host=os.environ.get("REDIS_HOST", "127.0.0.1"), port=6379, decode_responses=True)

# Config from Environment Variables
PROJECT_ID = "microservices-demo-adelia"
REGION = "europe-west1"
SNAPSHOT_BUCKET = os.environ.get("SNAPSHOT_BUCKET")
TASK_QUEUE = os.environ.get("TASK_QUEUE")
PROCESSOR_URL = os.environ.get("PROCESSOR_URL")
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MIN", 5))

tasks_client = tasks_v2.CloudTasksClient()
storage_client = storage.Client()

@app.before_request
def rate_limit_middleware():
    """Phase 4: Middleware for Rate Limiting"""
    if request.path == "/publish" and request.method == "POST":
        player_id = request.headers.get("X-Player-ID", "anonymous")
        doc_ref = db.collection("rate_limits").document(player_id)
        
        @firestore.transactional
        def check_and_update(transaction, doc_ref):
            snapshot = doc_ref.get(transaction=transaction)
            data = snapshot.to_dict() if snapshot.exists else {"count": 0, "window_start": datetime.now(timezone.utc).timestamp()}
            
            now = datetime.now(timezone.utc).timestamp()
            if now - data["window_start"] > 60:
                data = {"count": 1, "window_start": now}
            elif data["count"] >= RATE_LIMIT:
                return False
            else:
                data["count"] += 1
            
            transaction.set(doc_ref, data)
            return True

        if not check_and_update(db.transaction(), doc_ref):
            return jsonify({"error": "Rate limit exceeded", "player_id": player_id}), 429

@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json()
    key = f"event:{os.environ.get('HOSTNAME', 'local')}:{datetime.now(timezone.utc).isoformat()}"
    r.setex(key, 3600, json.dumps(data))
    
    # Cloud Tasks: Delegate snapshot saving
    if TASK_QUEUE and PROCESSOR_URL:
        parent = tasks_client.queue_path(PROJECT_ID, REGION, TASK_QUEUE)
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": f"{PROCESSOR_URL}/process",
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"redis_key": key}).encode(),
            }
        }
        tasks_client.create_task(request={"parent": parent, "task": task})
    
    return jsonify({"status": "published", "redis_key": key})

@app.route("/process", methods=["POST"])
def process():
    """Phase 3: Save to Cloud Storage"""
    body = request.get_json()
    key = body.get("redis_key")
    val = r.get(key)
    if val:
        bucket = storage_client.bucket(SNAPSHOT_BUCKET)
        blob = bucket.blob(f"snapshots/{key}.json")
        blob.upload_from_string(val, content_type="application/json")
    return jsonify({"status": "snapshot_saved"}), 200

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)