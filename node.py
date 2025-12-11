from flask import Flask, request, jsonify
import os, threading, time, sys, requests

app = Flask(__name__)

if len(sys.argv) < 2:
    print("Usage: python node.py <port>")
    sys.exit(1)

PORT = sys.argv[1]
NODE_NUM = PORT[-1]  # assumes single digit 1..9
STORAGE = f"storage/node{NODE_NUM}"
MASTER = "http://127.0.0.1:4000"

os.makedirs(STORAGE, exist_ok=True)
running = True


def block_path(block_id: str) -> str:
    safe_id = block_id.replace("/", "_")
    return os.path.join(STORAGE, f"{safe_id}.blk")


@app.route("/block_store", methods=["POST"])
def block_store():
    data = request.get_json(force=True)
    block_id = data.get("block_id")
    content = data.get("data", "")

    if not block_id:
        return "missing block_id", 400

    try:
        path = block_path(block_id)
        with open(path, "w", encoding="utf-8", errors="ignore") as f:
            f.write(content)
        print(f"[NODE {PORT}] Stored block {block_id}")
        return "OK", 200
    except Exception as e:
        print(f"[NODE {PORT}] block_store error: {e}")
        return "Error", 500


@app.route("/block_fetch", methods=["POST"])
def block_fetch():
    data = request.get_json(force=True)
    block_id = data.get("block_id")
    if not block_id:
        return "missing block_id", 400

    path = block_path(block_id)
    if not os.path.exists(path):
        return "Not found", 404

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    return jsonify({"data": content})


@app.route("/block_delete", methods=["POST"])
def block_delete():
    data = request.get_json(force=True)
    block_id = data.get("block_id")
    if not block_id:
        return "missing block_id", 400

    path = block_path(block_id)
    if os.path.exists(path):
        os.remove(path)
        print(f"[NODE {PORT}] Deleted block {block_id}")
        return "OK", 200
    return "Not found", 404


@app.route("/store", methods=["POST"])
def store_legacy():
    data = request.get_json(force=True)
    filename = data.get("filename")
    content = data.get("data", "")
    try:
        with open(os.path.join(STORAGE, filename), "w", encoding="utf-8", errors="ignore") as f:
            f.write(content)
        print(f"[NODE {PORT}] (legacy) Stored {filename}")
        return "OK", 200
    except Exception as e:
        print(f"[NODE {PORT}] store error: {e}")
        return "Error", 500


@app.route("/download", methods=["POST"])
def download_legacy():
    data = request.get_json(force=True)
    name = data.get("filename")
    path = os.path.join(STORAGE, name)
    if not os.path.exists(path):
        return "Not found", 404
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return jsonify({"data": f.read()})


@app.route("/delete", methods=["POST"])
def delete_legacy():
    data = request.get_json(force=True)
    name = data.get("filename")
    path = os.path.join(STORAGE, name)
    if os.path.exists(path):
        os.remove(path)
        print(f"[NODE {PORT}] (legacy) Deleted {name}")
        return "OK", 200
    return "Not found", 404


@app.route("/shutdown", methods=["POST"])
def shutdown():
    global running
    running = False
    return "Shutting down", 200


def heartbeat():
    while running:
        try:
            requests.post(f"{MASTER}/heartbeat", json={"port": PORT}, timeout=1)
        except Exception:
            pass
        time.sleep(1)
    os._exit(0)


if __name__ == "__main__":
    print(f"[NODE {PORT}] Running, storage={STORAGE} (block-based)")
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(port=int(PORT))
