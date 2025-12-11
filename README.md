# NeoFS — Distributed File System (Educational)

Small distributed file system simulation with:
- Master server (placement, heartbeats, replication)
- Multiple DataNodes (block storage)
- Simple client API to upload/download files
- Tkinter GUI for monitoring & upload/download

## Files
- `master.py` — Master server (flask)
- `node.py` — Data node (flask)
- `client.py` — Simple client API
- `gui.py` — Tkinter GUI for monitoring & uploading
- `config/config.json` — Configuration (nodes list)
- `.gitignore` — Recommended ignores

## Requirements
- Python 3.8+
- Packages:
