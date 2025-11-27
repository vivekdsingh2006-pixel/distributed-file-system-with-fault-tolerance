import requests, os

MASTER_URL = "http://127.0.0.1:4000"


def upload_file(path, replication_factor):
    filename = os.path.basename(path)
    r = requests.post(MASTER_URL + "/upload", json={"filename": filename, "replication_factor": replication_factor})
    if r.status_code != 200:
        return f"Master upload error: {r.text}"
    nodes = r.json().get("store_in", [])
    with open(path, "rb") as f:
        content = f.read().decode("utf-8", errors="ignore")
    for p in nodes:
        try:
            requests.post(f"http://127.0.0.1:{p}/store", json={"filename": filename, "data": content}, timeout=10)
        except:
            pass
    return f"Uploaded to nodes: {nodes}"


def download_file(filename):
    r = requests.post(MASTER_URL + "/locate", json={"filename": filename})
    if r.status_code != 200:
        return "File not found"
    for p in r.json().get("nodes", []):
        try:
            rr = requests.post(f"http://127.0.0.1:{p}/download", json={"filename": filename}, timeout=8)
            if rr.status_code == 200:
                data = rr.json().get("data", "")
                os.makedirs("downloads", exist_ok=True)
                with open(os.path.join("downloads", filename), "w", encoding="utf-8", errors="ignore") as f:
                    f.write(data)
                return f"Downloaded from node {p}"
        except:
            pass
    return "Failed to download from all replicas"


def list_files():
    return requests.get(MASTER_URL + "/list").json()


def delete_file(filename):
    r = requests.post(MASTER_URL + "/delete", json={"filename": filename})
    if r.status_code != 200:
        return "Delete failed"
    return r.json()
