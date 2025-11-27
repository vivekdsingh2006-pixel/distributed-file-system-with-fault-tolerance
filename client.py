import requests
import os

MASTER = "http://127.0.0.1:4000"

# ---------------- UPLOAD ----------------
def upload_file(filepath):
    filename = os.path.basename(filepath)

    # Read file content
    with open(filepath, "r") as f:
        content = f.read()

    # Ask user for replication factor (optional)
    rep_factor = 2  # default

    payload = {
        "filename": filename,
        "replication_factor": rep_factor
    }

    # Send upload request to master
    resp = requests.post(MASTER + "/upload", json=payload)
    if resp.status_code != 200:
        return "Upload failed: " + resp.text

    nodes = resp.json().get("store_in", [])

    # Store file on each node
    for p in nodes:
        url = f"http://127.0.0.1:{p}/store"
        requests.post(url, json={"filename": filename, "data": content})

    return f"Stored on nodes: {nodes}"


# ---------------- DOWNLOAD ----------------
def download_file(filename):
    # Ask master where file is
    r = requests.post(MASTER + "/locate", json={"filename": filename})
    if r.status_code != 200:
        return "File not found"

    nodes = r.json()["nodes"]

    # Try each node until one responds
    for p in nodes:
        try:
            r = requests.post(f"http://127.0.0.1:{p}/download",
                              json={"filename": filename}, timeout=5)

            if r.status_code == 200:
                content = r.json()["data"]

                os.makedirs("downloads", exist_ok=True)
                path = f"downloads/{filename}"

                with open(path, "w") as f:
                    f.write(content)

                return path
        except:
            pass

    return "Download failed"


# ---------------- LIST FILES ----------------
def list_files():
    d = requests.get(MASTER + "/list").json()
    return [f"{f}  -> {lst}" for f, lst in d.items()]


# ---------------- DELETE ----------------
def delete_file(filename):
    resp = requests.post(MASTER + "/delete", json={"filename": filename})
    if resp.status_code != 200:
        return "Delete failed: " + resp.text

    return resp.json()


# ---------------- NODE STATUS ----------------
def get_node_status():
    d = requests.get(MASTER + "/status").json()
    return [{"id": p, "status": s} for p, s in d.items()]
