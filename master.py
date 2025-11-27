from flask import Flask, request, jsonify
import threading, time, json, random, requests

app = Flask(__name__)

with open("config/config.json", "r") as f:
    cfg = json.load(f)

HEARTBEAT_TIMEOUT = 5
MONITOR_INTERVAL = 2
CLIENT_TIMEOUT = 6

# node_port -> { alive: bool, last_heartbeat: ts }
nodes = {}
for n in cfg["nodes"]:
    port = str(n["port"])
    nodes[port] = {"alive": False, "last_heartbeat": 0}

# client_id -> last_heartbeat_ts
clients = {}

# filename -> [ports]
file_index = {}

lock = threading.Lock()


@app.route("/status", methods=["GET"])
def status():
    with lock:
        node_report = {p: ("UP" if info["alive"] else "DOWN") for p, info in nodes.items()}
        active_clients = len([c for c, t in clients.items() if time.time() - t <= CLIENT_TIMEOUT])
    return jsonify({"nodes": node_report, "active_clients": active_clients})


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json(force=True)
    port = str(data.get("port"))
    with lock:
        if port in nodes:
            nodes[port]["alive"] = True
            nodes[port]["last_heartbeat"] = time.time()
        else:
            # allow unknown node to register (optional)
            nodes[port] = {"alive": True, "last_heartbeat": time.time()}
    return "OK", 200


@app.route("/client_heartbeat", methods=["POST"])
def client_heartbeat():
    data = request.get_json(force=True)
    cid = data.get("id")
    if not cid:
        return "missing id", 400
    with lock:
        clients[cid] = time.time()
    return "OK", 200


@app.route("/upload", methods=["POST"])
def upload():
    data = request.get_json(force=True)
    filename = data.get("filename")
    rep = int(data.get("replication_factor", 1))
    with lock:
        alive_nodes = [p for p, info in nodes.items() if info["alive"]]
        if rep > len(alive_nodes):
            return f"Not enough alive nodes ({len(alive_nodes)} available)", 500
        chosen = random.sample(alive_nodes, rep)
        file_index[filename] = chosen.copy()
    return jsonify({"store_in": chosen})


@app.route("/locate", methods=["POST"])
def locate():
    data = request.get_json(force=True)
    filename = data.get("filename")
    with lock:
        if filename not in file_index:
            return "File not found", 404
        lst = file_index[filename]
        alive = [p for p in lst if nodes.get(p, {}).get("alive")]
        dead = [p for p in lst if not nodes.get(p, {}).get("alive")]
    return jsonify({"nodes": alive + dead})


@app.route("/delete", methods=["POST"])
def delete():
    data = request.get_json(force=True)
    filename = data.get("filename")
    with lock:
        if filename not in file_index:
            return "File not found", 404
        targets = file_index[filename].copy()
    # attempt to delete on each node (best-effort)
    for p in targets:
        try:
            requests.post(f"http://127.0.0.1:{p}/delete", json={"filename": filename}, timeout=2)
        except:
            pass
    with lock:
        if filename in file_index:
            del file_index[filename]
    return jsonify({"deleted_from": targets})


@app.route("/list", methods=["GET"])
def list_files():
    with lock:
        return jsonify(file_index)


def monitor_loop():
    while True:
        time.sleep(MONITOR_INTERVAL)
        now = time.time()
        with lock:
            # mark nodes down if their heartbeat is stale
            for port, info in list(nodes.items()):
                if info["alive"] and (now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT):
                    info["alive"] = False
                    print(f"[MASTER] Node {port} went DOWN")
            # cleanup dead clients
            dead_clients = [c for c, t in clients.items() if now - t > CLIENT_TIMEOUT]
            for c in dead_clients:
                del clients[c]


threading.Thread(target=monitor_loop, daemon=True).start()

if __name__ == "__main__":
    print("[MASTER] Running at 4000")
    app.run(port=4000)
