import os, json, redis, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from google.cloud import tasks_v2, storage, firestore, pubsub_v1

# 1. DEFINE CONFIG FIRST
PROJECT_ID = "microservices-demo-adelia"
REGION = "europe-west1"
SNAPSHOT_BUCKET = os.environ.get("SNAPSHOT_BUCKET")
TASK_QUEUE = os.environ.get("TASK_QUEUE")
PROCESSOR_URL = os.environ.get("PROCESSOR_URL")
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MIN", 5))
SERVER_ID = os.environ.get("HOSTNAME", "local") # <-- FIXED 1: Defined SERVER_ID

# 2. INITIALIZE CLIENTS
publisher = pubsub_v1.PublisherClient()
TOPIC_PATH = publisher.topic_path(PROJECT_ID, "game-events") 
db = firestore.Client()
storage_client = storage.Client()
tasks_client = tasks_v2.CloudTasksClient()

r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "127.0.0.1"), 
    port=6379, 
    decode_responses=True
)

app = Flask(__name__)

# --- FIXED 2: The Missing Analytics Background Thread ---
def _update_analytics_async(player_id: str):
    """Mise a jour analytics en arriere-plan, ne bloque pas /publish."""
    def _write():
        try:
            doc_ref = db.collection("analytics").document(player_id)
            doc_ref.set({
                "total_requests": firestore.Increment(1),
                "last_seen":      datetime.now(timezone.utc),
            }, merge=True)
        except Exception as e:
            app.logger.warning(f"Analytics write failed: {e}")
    threading.Thread(target=_write, daemon=True).start()

# --- ROUTES ---

@app.before_request
def rate_limit_middleware():
    """Phase 4: Firestore Rate Limiting via Transaction"""
    if request.path == "/publish" and request.method == "POST":
        player_id = request.headers.get("X-Player-ID", "anonymous")
        doc_ref = db.collection("rate_limits").document(player_id)
        
        @firestore.transactional
        def check_and_update(transaction, doc_ref):
            snapshot = doc_ref.get(transaction=transaction)
            now = datetime.now(timezone.utc).timestamp()
            
            if snapshot.exists:
                data = snapshot.to_dict()
                if now - data["window_start"] > 60: # 60s window
                    data = {"count": 1, "window_start": now}
                elif data["count"] >= RATE_LIMIT:
                    return False
                else:
                    data["count"] += 1
            else:
                data = {"count": 1, "window_start": now}
            
            transaction.set(doc_ref, data)
            return True

        if not check_and_update(db.transaction(), doc_ref):
            return jsonify({"error": "Rate limit exceeded"}), 429

@app.route("/publish", methods=["POST"])
def publish():
    data = request.get_json()
    key = f"event:{SERVER_ID}:{datetime.now(timezone.utc).isoformat()}"
    r.setex(key, 3600, json.dumps(data))
    
    publisher.publish(TOPIC_PATH, key.encode())

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
    
    # --- FIXED 3: Actually trigger the analytics ---
    player_id = request.headers.get("X-Player-ID", "anonymous")
    _update_analytics_async(player_id)
    
    return jsonify({"status": "published", "redis_key": key})

@app.route("/process", methods=["POST"])
def process():
    """Cloud Task handler: Save snapshot to Storage"""
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

@app.route("/analytics")
def analytics():
    if request.headers.get("X-Admin-Key") != os.environ.get("ADMIN_KEY", "td-secret-2026"):
        return jsonify({"error": "Unauthorized"}), 401

    results = {}
    for doc in db.collection("analytics").stream():
        results[doc.id] = doc.to_dict()

    quotas = {}
    for doc in db.collection("rate_limits").stream():
        quotas[doc.id] = doc.to_dict()

    return jsonify({
        "server_id":  SERVER_ID,
        "analytics": results,
        "quotas":    quotas
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)