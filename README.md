📁 Distributed File System with Fault Tolerance

A simple, terminal-based DFS that replicates files across multiple storage nodes and continues working even if some nodes fail.

🚀 Overview

This project implements a simplified Distributed File System (DFS) that stores files across multiple independent storage nodes.
It includes:

A Master Server that handles metadata, replication, and node health.

Multiple Storage Nodes that store file blocks.

A Client Program to upload, download, list, and delete files.

Basic fault tolerance using replication and node-health monitoring.

This project is ideal for learning how distributed systems work internally.

🧱 Features
✔ File Upload

Splits file into chunks and distributes them across nodes with replication.

✔ File Download

Reconstructs the file using metadata from the master.

✔ Fault Tolerance

Files are stored with replication factor = 2

If one node fails, data is still available from another node.

✔ Node Health Monitoring

Each node sends heartbeat signals to master.
Master identifies UP/DOWN nodes.

✔ File Listing + Deletion

See all uploaded files and delete them from DFS.

📂 Project Structure
dfs-project/
│
|__ gui.py
├── master.py          # Master server (metadata manager)
├── node.py            # Storage node server
├── client.py          # Client for upload/download
│
├── config/
│   └── nodes.json     # Node IPs, ports & health status
│
└── storage/
    ├── node1/
    ├── node2/
    ├── node3/
    ├── node4/
    └── node5/
    Technologies & Concepts Used

Python (Sockets, JSON)

Distributed System Design

Replication

Fault Tolerance

Metadata Management

Heartbeat Monitoring

Great for internships + system design learning!