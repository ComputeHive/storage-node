"""
Minimal Flask backend that implements just enough of the
Decentorage API to let the Host-Node run end-to-end locally.

Run:
    pip install flask
    python dummy_backend.py

Listens on http://localhost:5000
"""

import uuid
import hashlib
import time
import threading
from flask import Flask, request, jsonify

app = Flask(__name__)

# ── In-Memory State ────────────────────────────────────────

registered_hosts = {}       # token → host info
active_shards = {}          # shard_id → { host_token, status, uploaded_at }
heartbeat_log = {}          # token → last_heartbeat_timestamp
withdraw_log = []           # list of { shard_id, token, timestamp }

TOKEN_SECRET = "dummy-secret-key"
lock = threading.Lock()


# ── Helpers ────────────────────────────────────────────────

def generate_token(username: str) -> str:
    raw = f"{username}:{TOKEN_SECRET}:{uuid.uuid4().hex}"
    return hashlib.sha256(raw.encode()).hexdigest()


def extract_token() -> str:
    return request.headers.get("token", "")


def is_authenticated() -> bool:
    token = extract_token()
    return token in registered_hosts


# ── Dummy User Accounts ───────────────────────────────────
# Add your test accounts here

VALID_ACCOUNTS = {
    "alice": "password123",
    "bob": "secret456",
    "host1": "host1pass",
    "test": "test",
    "admin": "admin",
}


# ══════════════════════════════════════════════════════════════
#  API Routes
# ══════════════════════════════════════════════════════════════

@app.route("/storage/signin", methods=["POST"])
def signin():
    body = request.get_json()
    username = body.get("username", "")
    password = body.get("password", "")

    if username in VALID_ACCOUNTS and VALID_ACCOUNTS[username] == password:
        token = generate_token(username)

        with lock:
            registered_hosts[token] = {
                "username": username,
                "ip_address": "",
                "port": 0,
                "registered_at": time.time(),
            }

        print(f"[AUTH] {username} logged in → token={token[:16]}...")
        return jsonify({"token": token}), 200

    print(f"[AUTH] FAILED login for '{username}'")
    return jsonify({"error": "Invalid credentials"}), 401


@app.route("/storage/heartbeat", methods=["GET"])
def heartbeat():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    token = extract_token()
    with lock:
        heartbeat_log[token] = time.time()
        username = registered_hosts[token]["username"]

    print(f"[PULSE] Heartbeat from {username}")
    return jsonify({"status": "alive"}), 200


@app.route("/storage/updateConnection", methods=["POST"])
def update_connection():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    token = extract_token()
    body = request.get_json()
    ip = body.get("ip_address", "")
    port = body.get("port", 0)

    with lock:
        registered_hosts[token]["ip_address"] = ip
        registered_hosts[token]["port"] = port
        username = registered_hosts[token]["username"]

    print(f"[NET] {username} updated connection → {ip}:{port}")
    return jsonify({"status": "updated"}), 200


@app.route("/storage/shardDoneUploading", methods=["POST"])
def shard_done_uploading():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    token = extract_token()
    body = request.get_json()
    shard_id = body.get("shard_id", "")

    with lock:
        active_shards[shard_id] = {
            "host_token": token,
            "status": "stored",
            "uploaded_at": time.time(),
        }
        username = registered_hosts[token]["username"]

    print(f"[SHARD] {username} confirmed storage of '{shard_id}'")
    return jsonify({"status": "confirmed"}), 200


@app.route("/storage/activeContracts", methods=["GET"])
def active_contracts():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    token = extract_token()

    with lock:
        host_shards = [
            sid for sid, info in active_shards.items()
            if info["host_token"] == token
        ]
        username = registered_hosts[token]["username"]

    print(f"[CONTRACTS] {username} queried → {len(host_shards)} active shards")
    return jsonify({"shards": host_shards}), 200


@app.route("/storage/withdraw", methods=["POST"])
def withdraw():
    if not is_authenticated():
        return jsonify({"error": "unauthorized"}), 401

    token = extract_token()
    body = request.get_json()
    shard_id = body.get("shard_id", "")

    with lock:
        withdraw_log.append({
            "shard_id": shard_id,
            "token": token,
            "timestamp": time.time(),
        })
        username = registered_hosts[token]["username"]

    print(f"[PAYMENT] {username} claimed payment for '{shard_id}'")
    return jsonify({"status": "payment_queued"}), 200


# ══════════════════════════════════════════════════════════════
#  Debug / Admin Routes (for testing)
# ══════════════════════════════════════════════════════════════

@app.route("/debug/hosts", methods=["GET"])
def debug_hosts():
    with lock:
        hosts = {
            token[:16]: info for token, info in registered_hosts.items()
        }
    return jsonify(hosts), 200


@app.route("/debug/shards", methods=["GET"])
def debug_shards():
    with lock:
        return jsonify(active_shards), 200


@app.route("/debug/heartbeats", methods=["GET"])
def debug_heartbeats():
    with lock:
        beats = {
            token[:16]: timestamp
            for token, timestamp in heartbeat_log.items()
        }
    return jsonify(beats), 200


@app.route("/debug/withdrawals", methods=["GET"])
def debug_withdrawals():
    with lock:
        return jsonify(withdraw_log), 200


@app.route("/debug/add_shard", methods=["POST"])
def debug_add_shard():
    """
    Manually register a shard as active for a host.
    Useful for testing uploads — pretend a contract exists.

    POST /debug/add_shard
    Body: {"shard_id": "test-shard-001", "token_prefix": "abc123"}
    """
    body = request.get_json()
    shard_id = body.get("shard_id", "")
    token_prefix = body.get("token_prefix", "")

    with lock:
        matching_token = None
        for token in registered_hosts:
            if token.startswith(token_prefix):
                matching_token = token
                break

        if matching_token is None:
            return jsonify({"error": "no host matches that token prefix"}), 404

        active_shards[shard_id] = {
            "host_token": matching_token,
            "status": "active",
            "uploaded_at": time.time(),
        }

    print(f"[DEBUG] Manually added shard '{shard_id}'")
    return jsonify({"status": "added"}), 200


@app.route("/debug/reset", methods=["POST"])
def debug_reset():
    """Clear all state — start fresh."""
    with lock:
        registered_hosts.clear()
        active_shards.clear()
        heartbeat_log.clear()
        withdraw_log.clear()

    print("[DEBUG] All state reset")
    return jsonify({"status": "reset"}), 200


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "service": "Dummy Decentorage Backend",
        "endpoints": [
            "POST /storage/signin",
            "GET  /storage/heartbeat",
            "POST /storage/updateConnection",
            "POST /storage/shardDoneUploading",
            "GET  /storage/activeContracts",
            "POST /storage/withdraw",
            "",
            "GET  /debug/hosts",
            "GET  /debug/shards",
            "GET  /debug/heartbeats",
            "GET  /debug/withdrawals",
            "POST /debug/add_shard",
            "POST /debug/reset",
        ],
    }), 200


# ══════════════════════════════════════════════════════════════
#  Entry Point
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  DUMMY DECENTORAGE BACKEND")
    print("=" * 60)
    print()
    print("  Test accounts:")
    for user, pwd in VALID_ACCOUNTS.items():
        print(f"    {user} / {pwd}")
    print()
    print("  Debug dashboard: http://localhost:5000/debug/hosts")
    print("=" * 60)
    print()

    app.run(host="0.0.0.0", port=5000, debug=True)