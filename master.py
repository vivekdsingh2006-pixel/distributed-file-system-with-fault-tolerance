from flask import Flask, request, jsonify
import threading, time, json, random, requests

app = Flask(__name__)

with open("config/config.json", "r") as f:
    cfg = json.load(f)

HEARTBEAT_TIMEOUT = 5
MONITOR_INTERVAL = 2
CLIENT_TIMEOUT = 6

# Node state: node_port -> { alive: bool, last_heartbeat: ts }
nodes = {}
for n in cfg["nodes"]:
    port = str(n["port"])
    nodes[port] = {"alive": False, "last_heartbeat": 0}

# client_id -> last_heartbeat_ts
clients = {}

# filename -> metadata
# {
#   "replication_factor": int,
#   "size": int,
#   "block_size": int,
#   "blocks": [
#       { "id": "filename__blk0", "replicas": ["5001", "5003"] },
#       ...
#   ]
# }
file_index = {}

# Default block size (bytes) – must match client/gui
BLOCK_SIZE = 64 * 1024

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
    """
    Initialize an upload: client has already split file into blocks.

    Request JSON:
    {
        "filename": "foo.txt",
        "replication_factor": 3,
        "num_blocks": 5,
        "size": 12345     # optional but recommended
    }

    Response JSON:
    {
        "filename": "foo.txt",
        "replication_factor": 3,
        "block_size": 65536,
        "blocks": [
            { "id": "foo.txt__blk0", "nodes": ["5001","5003","5005"] },
            ...
        ]
    }
    """
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return "missing filename", 400

    rep = int(data.get("replication_factor", 1))
    num_blocks = int(data.get("num_blocks", 1))
    size = int(data.get("size", 0))

    if num_blocks <= 0:
        return "num_blocks must be > 0", 400

    with lock:
        alive_nodes = [p for p, info in nodes.items() if info["alive"]]
        if not alive_nodes:
            return "No alive nodes available", 500

        if rep > len(alive_nodes):
            return f"Not enough alive nodes ({len(alive_nodes)} available)", 500

        blocks_meta = []
        for i in range(num_blocks):
            chosen = random.sample(alive_nodes, rep)
            block_id = f"{filename}__blk{i}"
            blocks_meta.append({"id": block_id, "replicas": chosen})

        file_index[filename] = {
            "replication_factor": rep,
            "size": size,
            "block_size": BLOCK_SIZE,
            "blocks": blocks_meta,
        }

    # For the client/GUI we return block_id + node assignments
    response_blocks = [{"id": b["id"], "nodes": b["replicas"]} for b in blocks_meta]
    return jsonify(
        {
            "filename": filename,
            "replication_factor": rep,
            "block_size": BLOCK_SIZE,
            "blocks": response_blocks,
        }
    )


@app.route("/locate", methods=["POST"])
def locate():
    """
    Locate all blocks + replicas for a given file.

    Request:
        { "filename": "foo.txt" }

    Response:
    {
        "filename": "foo.txt",
        "size": 12345,
        "block_size": 65536,
        "replication_factor": 3,
        "blocks": [
            { "id": "foo.txt__blk0", "nodes": ["alive-first", "dead-later"...] },
            ...
        ]
    }
    """
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return "missing filename", 400

    with lock:
        meta = file_index.get(filename)
        if not meta:
            return "File not found", 404

        blocks = []
        for b in meta["blocks"]:
            reps = b["replicas"]
            alive_rep = [p for p in reps if nodes.get(p, {}).get("alive")]
            dead_rep = [p for p in reps if p not in alive_rep]
            blocks.append({"id": b["id"], "nodes": alive_rep + dead_rep})

        return jsonify(
            {
                "filename": filename,
                "size": meta.get("size", 0),
                "block_size": meta.get("block_size", BLOCK_SIZE),
                "replication_factor": meta.get("replication_factor", 1),
                "blocks": blocks,
            }
        )


@app.route("/delete", methods=["POST"])
def delete():
    """
    Delete a file: remove all of its blocks from all replicas.
    """
    data = request.get_json(force=True)
    filename = data.get("filename")
    if not filename:
        return "missing filename", 400

    with lock:
        meta = file_index.get(filename)
        if not meta:
            return "File not found", 404
        blocks = list(meta["blocks"])  # copy
        del file_index[filename]

    deleted_from = {}
    for b in blocks:
        block_id = b["id"]
        for p in b["replicas"]:
            try:
                r = requests.post(
                    f"http://127.0.0.1:{p}/block_delete",
                    json={"block_id": block_id},
                    timeout=2,
                )
                deleted_from.setdefault(p, []).append(block_id)
            except Exception:
                # best-effort
                pass

    return jsonify({"filename": filename, "deleted_from": deleted_from})


@app.route("/list", methods=["GET"])
def list_files():
    """
    Show file -> metadata (including blocks & replicas).
    """
    with lock:
        # Return a light-weight view
        out = {}
        for fname, meta in file_index.items():
            out[fname] = {
                "replication_factor": meta.get("replication_factor", 1),
                "size": meta.get("size", 0),
                "block_size": meta.get("block_size", BLOCK_SIZE),
                "num_blocks": len(meta.get("blocks", [])),
                "blocks": meta.get("blocks", []),
            }
        return jsonify(out)


def _snapshot_state_for_replication():
    """
    Take a snapshot of nodes + file_index for use in replication loop
    without holding the global lock while doing HTTP calls.
    """
    with lock:
        alive_nodes = [p for p, info in nodes.items() if info["alive"]]
        # Shallow-ish copy; blocks lists contain only primitives
        files_copy = json.loads(json.dumps(file_index))
    return alive_nodes, files_copy


def _perform_re_replication(alive_nodes, files_copy):
    """
    For any block that has fewer alive replicas than replication_factor,
    create new replicas on other alive nodes.
    """
    if not alive_nodes:
        return

    for filename, meta in files_copy.items():
        rf = meta.get("replication_factor", 1)
        for b in meta.get("blocks", []):
            block_id = b["id"]
            replicas = b["replicas"]
            alive_reps = [p for p in replicas if p in alive_nodes]

            if len(alive_reps) >= rf:
                continue  # already satisfied

            # Need to create more replicas
            candidates = [p for p in alive_nodes if p not in replicas]
            if not candidates or not alive_reps:
                # Either no source or no place to put new replica
                continue

            src = alive_reps[0]
            dst = random.choice(candidates)

            try:
                # Fetch block content from src
                rr = requests.post(
                    f"http://127.0.0.1:{src}/block_fetch",
                    json={"block_id": block_id},
                    timeout=3,
                )
                if rr.status_code != 200:
                    continue
                data = rr.json().get("data", "")

                # Store on dst
                wr = requests.post(
                    f"http://127.0.0.1:{dst}/block_store",
                    json={"block_id": block_id, "data": data},
                    timeout=3,
                )
                if wr.status_code != 200:
                    continue

                # Update real metadata under lock
                with lock:
                    real_meta = file_index.get(filename)
                    if not real_meta:
                        continue
                    real_block = next(
                        (bb for bb in real_meta["blocks"] if bb["id"] == block_id), None
                    )
                    if real_block and dst not in real_block["replicas"]:
                        real_block["replicas"].append(dst)
                        print(
                            f"[MASTER] Re-replicated block {block_id} from {src} -> {dst}"
                        )

            except Exception as e:
                # Ignore and try next time
                print(f"[MASTER] Re-replication error for {block_id}: {e}")


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

        # After updating node states, do re-replication checks
        alive_nodes, files_copy = _snapshot_state_for_replication()
        _perform_re_replication(alive_nodes, files_copy)


threading.Thread(target=monitor_loop, daemon=True).start()

if __name__ == "__main__":
    print("[MASTER] Running at 4000 with block-based DFS")
    app.run(port=4000)
