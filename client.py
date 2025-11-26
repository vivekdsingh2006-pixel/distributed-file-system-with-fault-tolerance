import requests

MASTER = "http://127.0.0.1:4000"

def delete_file():
    name = input("Enter filename to delete: ")

    resp = requests.post(MASTER + "/delete", json={"filename": name})
    
    if resp.status_code != 200:
        print("Delete failed:", resp.text)
        return
    
    deleted = resp.json().get("deleted_from", [])
    print("Deleted from nodes:", deleted)

def upload():
    filename = input("Enter file name to upload (e.g. notes.txt): ").strip()
    if not filename:
        print("Filename required")
        return

    content = input("Enter file content: ")

    try:
        rep_factor = int(input("Enter replication factor (1 to 5): ").strip())
    except:
        print("Invalid number")
        return

    payload = {
        "filename": filename,
        "replication_factor": rep_factor
    }

    resp = requests.post(MASTER + "/upload", json=payload)
    if resp.status_code != 200:
        print("Upload failed:", resp.text)
        return

    nodes = resp.json().get("store_in", [])
    print("Master -> store in nodes:", nodes)

    for p in nodes:
        node_url = f"http://127.0.0.1:{p}/store"
        try:
            r = requests.post(node_url, json={"filename": filename, "data": content}, timeout=5)
            if r.status_code == 200:
                print(f"Stored on node {p}")
            else:
                print(f"Node {p} store failed: {r.status_code} {r.text}")
        except Exception as e:
            print(f"Error contacting node {p}: {e}")

def download():
    name = input("Filename: ")

    r = requests.post(MASTER + "/locate", json={"filename": name})
    if r.status_code != 200:
        print("Not found")
        return

    nodes = r.json()["nodes"]

    for p in nodes:
        try:
            r = requests.post(f"http://127.0.0.1:{p}/download",
                              json={"filename": name}, timeout=5)
            if r.status_code == 200:
                print("Content:\n", r.json()["data"])
                return
        except:
            pass

    print("Could not download")


def list_files():
    d = requests.get(MASTER + "/list").json()
    for f, lst in d.items():
        print(f"{f} -> {lst}")


def status():
    d = requests.get(MASTER + "/status").json()
    for p, s in d.items():
        print(p, s)


if __name__ == "__main__":
    while True:
        print("\n1 Upload\n2 Download\n3 List\n4 Status\n5 Delete\n6 Exit")
        ch = input("Choice: ")
        if ch == "1":
            upload()
        elif ch == "2":
            download()
        elif ch == "3":
            list_files()
        elif ch == "4":
            status()
        elif ch == "5":
            delete_file()
        else:
            break
