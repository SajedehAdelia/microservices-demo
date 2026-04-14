import os, json, redis, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
from google.cloud import pubsub_v1

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Config
PROJECT_ID = "microservices-demo-adelia"
TOPIC_ID = "td-redis-topic"
# Each instance needs its OWN subscription name (passed via Env Var later)
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_NAME")

r = redis.Redis(host=os.environ.get("REDIS_HOST", "127.0.0.1"), 
                port=6379, decode_responses=True)
SERVER_ID = os.environ.get("HOSTNAME", "local")

# Pub/Sub Setup
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def callback(message):
    """Triggered when a message arrives via Pub/Sub"""
    event_key = message.data.decode("utf-8")
    data = r.get(event_key)
    if data:
        # Push to all WebSockets connected to THIS instance
        socketio.emit('update', json.loads(data))
    message.ack() # Remove from queue [cite: 398]

def listen_pubsub():
    """Background thread to listen for events"""
    if SUBSCRIPTION_ID:
        subscriber = pubsub_v1.SubscriberClient()
        sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
        streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
        with subscriber:
            try: streaming_pull_future.result()
            except Exception as e: print(f"Listening failed: {e}")

@app.route("/publish", methods=["POST"])
def publish():
    msg_text = request.get_json().get("message")
    entry = {"message": msg_text, "server_id": SERVER_ID, 
             "published_at": datetime.now(timezone.utc).isoformat()}
    key = f"event:{SERVER_ID}:{entry['published_at']}"
    r.setex(key, 3600, json.dumps(entry))
    # Notify other instances via Pub/Sub [cite: 400]
    publisher.publish(topic_path, key.encode("utf-8"))
    return jsonify({"status": "published", "data": entry})

@socketio.on('connect')
def handle_connect():
    """Send current state to new players [cite: 394]"""
    result = []
    cursor, keys = r.scan(match="event:*")
    for k in keys:
        val = r.get(k)
        if val: result.append(json.loads(val))
    emit('initial_state', {"server_id": SERVER_ID, "events": result})

if __name__ == "__main__":
    threading.Thread(target=listen_pubsub, daemon=True).start()
    socketio.run(app, host="0.0.0.0", port=8080)