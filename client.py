import requests, os

MASTER_URL = "http://127.0.0.1:4000"

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

    os.makedirs("downloads", exist_ok=True)
    out_path = os.path.join("downloads", filename)
    with open(out_path, "w", encoding="utf-8", errors="ignore") as f:
        f.write("".join(assembled))

    return f"Downloaded to {out_path}"


def list_files():
    return requests.get(MASTER_URL + "/list").json()


def delete_file(filename):
    r = requests.post(MASTER_URL + "/delete", json={"filename": filename})
    if r.status_code != 200:
        return "Delete failed: " + r.text
    return r.json()


if __name__ == "__main__":
    pass
