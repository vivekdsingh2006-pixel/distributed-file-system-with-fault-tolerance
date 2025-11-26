from flask import Flask, request, jsonify
import os
import threading
import time
import sys
import requests

app = Flask(__name__)

if len(sys.argv) != 2:
    print("Usage: python node.py <port>")
    sys.exit(1)

PORT = sys.argv[1]
NODE_NUM = PORT[-1]
STORAGE_PATH = f"storage/node{NODE_NUM}"
MASTER = "http://127.0.0.1:4000"

os.makedirs(STORAGE_PATH, exist_ok=True)


@app.route("/store", methods=["POST"])
def store():
    d = request.get_json(force=True)
    filename = d["filename"]
    data = d.get("data", "")

    with open(f"{STORAGE_PATH}/{filename}", "w") as f:
        f.write(data)

    print(f"[NODE {PORT}] Stored {filename}")
    return "OK", 200


@app.route("/download", methods=["POST"])
def download():
    name = request.get_json(force=True)["filename"]
    path = f"{STORAGE_PATH}/{name}"

    if not os.path.exists(path):
        return "Not found", 404

    return jsonify({"data": open(path).read()})


@app.route("/replicate", methods=["POST"])
def replicate():
    d = request.get_json(force=True)
    filename = d["filename"]
    source = d["source_port"]

    try:
        r = requests.post(f"http://127.0.0.1:{source}/download",
                          json={"filename": filename}, timeout=5)

        if r.status_code != 200:
            return "Source failed", 500

        content = r.json()["data"]

        with open(f"{STORAGE_PATH}/{filename}", "w") as f:
            f.write(content)

        print(f"[NODE {PORT}] Replicated {filename} from {source}")
        return "OK", 200

    except Exception as e:
        return f"Error: {e}", 500


def heartbeat():
    while True:
        try:
            requests.post(MASTER + "/heartbeat",
                          json={"port": PORT}, timeout=2)
        except:
            pass
        time.sleep(1)


if __name__ == "__main__":
    print(f"[NODE {PORT}] Running at storage {STORAGE_PATH}")
    threading.Thread(target=heartbeat, daemon=True).start()
    app.run(port=int(PORT))
