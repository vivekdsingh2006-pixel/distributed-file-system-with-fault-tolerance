import requests, os

MASTER_URL = "http://127.0.0.1:4000"
<<<<<<< HEAD

# Must match master.py BLOCK_SIZE
BLOCK_SIZE = 64 * 1024


def _split_file_into_blocks(path):
    with open(path, "rb") as f:
        data = f.read()
    size = len(data)
    blocks = []
    for i in range(0, size, BLOCK_SIZE):
        chunk_bytes = data[i : i + BLOCK_SIZE]
        chunk_str = chunk_bytes.decode("utf-8", errors="ignore")
        blocks.append(chunk_str)
    return size, blocks


def upload_file(path, replication_factor):
    filename = os.path.basename(path)
    if not os.path.exists(path):
        return f"Path not found: {path}"

    size, blocks = _split_file_into_blocks(path)
    num_blocks = len(blocks)

    # Step 1: ask master for block placements
    r = requests.post(
        MASTER_URL + "/upload",
        json={
            "filename": filename,
            "replication_factor": replication_factor,
            "num_blocks": num_blocks,
            "size": size,
        },
    )
    if r.status_code != 200:
        return f"Master upload error: {r.text}"

    meta = r.json()
    block_metas = meta.get("blocks", [])
    if len(block_metas) != num_blocks:
        return "Master returned inconsistent block mapping"

    # Step 2: push each block to assigned DataNodes
    for idx, (block_data, bmeta) in enumerate(zip(blocks, block_metas)):
        block_id = bmeta["id"]
        nodes = bmeta["nodes"]
        for p in nodes:
            try:
                rr = requests.post(
                    f"http://127.0.0.1:{p}/block_store",
                    json={"block_id": block_id, "data": block_data},
                    timeout=10,
                )
                if rr.status_code != 200:
                    print(f"[WARN] Node {p} failed for block {block_id}: {rr.text}")
            except Exception as e:
                print(f"[WARN] Error pushing block {block_id} to node {p}: {e}")

    return f"Uploaded {filename} as {num_blocks} blocks, RF={replication_factor}"


def download_file(filename):
    # Get block locations from Master
    r = requests.post(MASTER_URL + "/locate", json={"filename": filename})
    if r.status_code != 200:
        return "File not found"

    meta = r.json()
    blocks = meta.get("blocks", [])

    assembled = []
    for b in blocks:
        block_id = b["id"]
        nodes = b["nodes"]
        block_data = None

        for p in nodes:
            try:
                rr = requests.post(
                    f"http://127.0.0.1:{p}/block_fetch",
                    json={"block_id": block_id},
                    timeout=8,
                )
                if rr.status_code == 200:
                    block_data = rr.json().get("data", "")
                    break
            except Exception:
                continue

        if block_data is None:
            return f"Failed to download block {block_id} from all replicas"

        assembled.append(block_data)

    # Join blocks and save to downloads
    os.makedirs("downloads", exist_ok=True)
    out_path = os.path.join("downloads", filename)
    with open(out_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("".join(assembled))

    return f"Downloaded to {out_path}"
=======


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
>>>>>>> 9a09f7621a4d8ff6b5ac875d70bb06a6bd10d679


def list_files():
    return requests.get(MASTER_URL + "/list").json()


def delete_file(filename):
    r = requests.post(MASTER_URL + "/delete", json={"filename": filename})
    if r.status_code != 200:
<<<<<<< HEAD
        return "Delete failed: " + r.text
    return r.json()


if __name__ == "__main__":
    # Simple manual test usage example (optional)
    # print(upload_file("test.txt", 3))
    # print(download_file("test.txt"))
    # print(list_files())
    # print(delete_file("test.txt"))
    pass
=======
        return "Delete failed"
    return r.json()
>>>>>>> 9a09f7621a4d8ff6b5ac875d70bb06a6bd10d679
