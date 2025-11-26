from flask import Flask, request, jsonify
import threading
import time
import requests
import json
import random

app = Flask(__name__)

# ---------- LOAD CONFIG ----------
with open("config/config.json", "r") as f:
    config = json.load(f)

REPLICATION_FACTOR = config["replication_factor"]
HEARTBEAT_TIMEOUT = 5
MONITOR_INTERVAL = 2

# Load node list from config.json
nodes = {}
for n in config["nodes"]:
    port = str(n["port"])
    nodes[port] = {"alive": True, "last_heartbeat": time.time()}

file_index = {}
lock = threading.Lock()


# ---------- ENDPOINTS ----------
@app.route("/status", methods=["GET"])
def status():
    with lock:
        report = {p: ("UP" if info["alive"] else "DOWN") for p, info in nodes.items()}
    return jsonify(report)

@app.route("/delete", methods=["POST"])
def delete_file():
    data = request.get_json(force=True)
    filename = data.get("filename")

    if not filename:
        return "Filename required", 400

    with lock:
        if filename not in file_index:
            return "File not found", 404

        nodes_to_delete = file_index[filename].copy()

    # Ask all nodes to delete the file
    for p in nodes_to_delete:
        try:
            url = f"http://127.0.0.1:{p}/delete"
            requests.post(url, json={"filename": filename}, timeout=5)
        except:
            pass

    with lock:
        del file_index[filename]

    print(f"[MASTER] Deleted file {filename} from system.")
    return jsonify({"deleted_from": nodes_to_delete})

@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.get_json(force=True)
    port = str(data.get("port"))

    with lock:
        if port in nodes:
            nodes[port]["alive"] = True
            nodes[port]["last_heartbeat"] = time.time()
        else:
            nodes[port] = {"alive": True, "last_heartbeat": time.time()}
    return "OK", 200


@app.route("/upload", methods=["POST"])
def upload():
    data = request.get_json(force=True)
    filename = data.get("filename")
    rep_factor = data.get("replication_factor", REPLICATION_FACTOR)

    if not filename:
        return "Bad Request", 400

    with lock:
        alive_nodes = [p for p, info in nodes.items() if info["alive"]]

        if rep_factor < 1:
            rep_factor = 1

        if rep_factor > len(alive_nodes):
            return f"Not enough alive nodes (requested {rep_factor}, available {len(alive_nodes)})", 500

        # pick nodes based on client choice
        chosen = random.sample(alive_nodes, rep_factor)

        file_index[filename] = chosen.copy()

    return jsonify({"store_in": chosen})


@app.route("/locate", methods=["POST"])
def locate():
    data = request.get_json(force=True)
    filename = data.get("filename")

    with lock:
        if filename not in file_index:
            return "File not found", 404

        stored = file_index[filename]
        alive = [p for p in stored if nodes[p]["alive"]]
        dead = [p for p in stored if not nodes[p]["alive"]]

    return jsonify({"nodes": alive + dead})


@app.route("/list", methods=["GET"])
def list_files():
    with lock:
        return jsonify(file_index)


# ---------- MONITOR ----------
def monitor_loop():
    while True:
        time.sleep(MONITOR_INTERVAL)
        now = time.time()
        rework = []

        with lock:
            for port, info in nodes.items():
                if info["alive"] and now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                    print(f"[MASTER] DEAD NODE DETECTED: {port}")
                    nodes[port]["alive"] = False

                    affected = [f for f, lst in file_index.items() if port in lst]
                    if affected:
                        rework.append((port, affected))

        for dead_port, files in rework:
            for fname in files:
                handle_re_replication(dead_port, fname)


def handle_re_replication(dead_port, filename):

    with lock:
        replicas = file_index[filename]
        alive_replicas = [p for p in replicas if nodes[p]["alive"]]
        candidates = [p for p in nodes if nodes[p]["alive"] and p not in replicas]

    if not alive_replicas or not candidates:
        print(f"[MASTER] Cannot re-replicate {filename} yet.")
        return

    source = alive_replicas[0]
    target = candidates[0]

    print(f"[MASTER] Re-replicating {filename}: {source} -> {target}")

    try:
        url = f"http://127.0.0.1:{target}/replicate"
        req = requests.post(url, json={"filename": filename, "source_port": source}, timeout=8)

        if req.status_code == 200:
            with lock:
                lst = file_index[filename]
                lst = [p for p in lst if p != dead_port]
                lst.append(target)
                file_index[filename] = lst
            print(f"[MASTER] Re-replication OK for {filename}")

        else:
            print("REPLICATION FAILED", req.text)

    except Exception as e:
        print("REPLICATION ERROR:", e)


threading.Thread(target=monitor_loop, daemon=True).start()

if __name__ == "__main__":
    print("[MASTER] Running on 4000")
    app.run(port=4000)
