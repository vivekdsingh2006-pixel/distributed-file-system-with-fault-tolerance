"""
Final NeoFS GUI (Fixed Stats + Active Clients + Failed Nodes)
Clean + merged version with corrected _poll_loop().
"""

import tkinter as tk
from tkinter import ttk, filedialog, simpledialog
import subprocess
import threading
import requests
import time
import os
import atexit

MASTER_URL = "http://127.0.0.1:4000"
NODE_PORTS = ["5001", "5002", "5003", "5004", "5005"]
BLOCK_SIZE = 64 * 1024
POLL_INTERVAL = 1.0

# ---------------- THEME (Dark default) ----------------
THEME = {
    "BG": "#0B0F1A",
    "CARD": "#0F1720",
    "ACCENT": "#2F3F79",
    "ACCENT_ALT": "#3B4BB0",
    "TEXT": "#E5E7EB",
    "MUTED": "#9CA3AF",
    "TEXTBOX_BG": "#020617",
    "LOG_BORDER": "#1F2937",
}

# ---------------- Modal ----------------
class GlassModal(tk.Toplevel):
    def __init__(self, parent, title, message):
        super().__init__(parent)
        self.transient(parent)
        self.grab_set()
        self.title(title)
        self.configure(bg=THEME["CARD"])
        self.geometry("480x200")
        self.resizable(False, False)

        inner = tk.Frame(self, bg=THEME["CARD"])
        inner.pack(fill="both", expand=True, padx=14, pady=14)

        tk.Label(inner, text=title, fg=THEME["TEXT"], bg=THEME["CARD"],
                 font=("Segoe UI", 12, "bold")).pack(anchor="w")

        tk.Label(inner, text=message, fg=THEME["TEXT"], bg=THEME["CARD"],
                 font=("Segoe UI", 10), wraplength=450).pack(anchor="w", pady=(6, 12))

        tk.Button(inner, text="OK", bg=THEME["ACCENT"], fg="white", bd=0,
                  padx=12, pady=6, command=self.destroy).pack(anchor="e")


# ---------------- GUI CLASS ----------------
class NeoFS_GUI:

    def __init__(self, root):
        self.root = root
        root.title("NeoFS")
        root.geometry("1120x750")
        root.configure(bg=THEME["BG"])

        self.master_process = None
        self.node_processes = {}
        self.polling = True

        # Build UI
        self._build_ui()

        # Auto-start master if needed
        self.start_master_if_needed()

        # Background threads
        threading.Thread(target=self._poll_loop, daemon=True).start()
        threading.Thread(target=self._client_heartbeat_loop, daemon=True).start()

        atexit.register(self.cleanup)

    # ---------------- THEME TOGGLER ----------------
    def toggle_theme(self):
        global THEME

        if THEME["BG"] == "#0B0F1A":  # dark → light
            THEME.update({
                "BG": "#E3FBF7",
                "CARD": "#8BBAC5",
                "ACCENT": "#4C5DAA",
                "ACCENT_ALT": "#3753a3",
                "TEXT": "#000000",
                "MUTED": "#374151",
                "TEXTBOX_BG": "#FFFFFF",
                "LOG_BORDER": "#c4c4c4",
            })
        else:
            THEME.update({
                "BG": "#0B0F1A",
                "CARD": "#0F1720",
                "ACCENT": "#2F3F79",
                "ACCENT_ALT": "#3B4BB0",
                "TEXT": "#E5E7EB",
                "MUTED": "#9CA3AF",
                "TEXTBOX_BG": "#020617",
                "LOG_BORDER": "#1F2937",
            })

        for w in self.root.winfo_children():
            w.destroy()
        self._build_ui()

    # ---------------- UI LAYOUT ----------------
    def _build_ui(self):

        # ---------- Sidebar ----------
        sidebar = tk.Frame(self.root, bg=THEME["ACCENT"], width=240)
        sidebar.pack(side="left", fill="y")

        tk.Label(sidebar, text="NeoFS", fg="white", bg=THEME["ACCENT"],
                 font=("Segoe UI", 20, "bold")).pack(pady=(16,6))
        tk.Label(sidebar, text="Distributed File System",
                 fg="white", bg=THEME["ACCENT"]).pack()

        menu = [
            ("Upload", self.open_upload_dialog),
            ("Download", self.download_dialog),
            ("List Files", self.list_files),
            ("Delete File", self.delete_dialog),
        ]

        for txt, cmd in menu:
            tk.Button(sidebar, text=txt, command=cmd,
                      bg=THEME["CARD"], fg=THEME["TEXT"], bd=0,
                      pady=10).pack(fill="x", padx=14, pady=8)

        tk.Button(sidebar, text="Toggle Theme", command=self.toggle_theme,
                  bg=THEME["ACCENT_ALT"], fg="white", bd=0,
                  pady=10).pack(fill="x", padx=14, pady=12)

        # ---------- Main Content Layout ----------
        main = tk.Frame(self.root, bg=THEME["BG"])
        main.pack(side="left", fill="both", expand=True, padx=14, pady=10)

        # Top Stats
        stats = tk.Frame(main, bg=THEME["BG"])
        stats.pack(fill="x", pady=(4,14))

        # Store references to stat cards
        self.card_total_nodes  = self._stat_card(stats, "Total Nodes", len(NODE_PORTS))
        self.card_total_nodes.pack(side="left", padx=8)

        self.card_active_clients = self._stat_card(stats, "Active Clients", 0)
        self.card_active_clients.pack(side="left", padx=8)

        self.card_failed_nodes = self._stat_card(stats, "Failed Nodes", 0)
        self.card_failed_nodes.pack(side="left", padx=8)

        # Body Grid (upload left, nodes right)
        body = tk.Frame(main, bg=THEME["BG"])
        body.pack(fill="both", expand=True)
        body.grid_columnconfigure(0, weight=3)
        body.grid_columnconfigure(1, weight=2)
        body.grid_rowconfigure(0, weight=1)

        # ---------- Upload Panel ----------
        left = tk.Frame(body, bg=THEME["BG"])
        left.grid(row=0, column=0, sticky="nsew", padx=(0,10))

        upload_card = tk.Frame(left, bg=THEME["CARD"])
        upload_card.pack(fill="both", expand=True)

        tk.Label(upload_card, text="File Upload", fg=THEME["TEXT"],
                 bg=THEME["CARD"], font=("Segoe UI", 12, "bold")
        ).pack(anchor="w", padx=12, pady=(10,4))

        self.drop = tk.Frame(upload_card, bg=THEME["TEXTBOX_BG"], height=160)
        self.drop.pack(padx=12, pady=10, fill="x")
        self.drop.pack_propagate(False)
        tk.Label(self.drop, text="Click to select file or drag here",
                 bg=THEME["TEXTBOX_BG"], fg=THEME["TEXT"]
        ).pack(expand=True)
        self.drop.bind("<Button-1>", lambda e: self.open_upload_dialog())

        self.chosen_label = tk.Label(upload_card, text="", bg=THEME["CARD"], fg=THEME["TEXT"])
        self.chosen_label.pack(anchor="w", padx=12)

        self.progress = ttk.Progressbar(upload_card, length=420, mode="determinate")
        self.progress.pack(padx=12, pady=(6,12))

        # ---------- Node Panel ----------
        right = tk.Frame(body, bg=THEME["BG"])
        right.grid(row=0, column=1, sticky="nsew")

        node_card = tk.Frame(right, bg=THEME["CARD"])
        node_card.pack(fill="both", expand=True)

        tk.Label(node_card, text="Node Panel", fg=THEME["TEXT"],
                 bg=THEME["CARD"], font=("Segoe UI", 12, "bold")
        ).pack(anchor="w", padx=12, pady=(10,4))

        # Start/Stop All
        np_top = tk.Frame(node_card, bg=THEME["CARD"])
        np_top.pack(anchor="w", padx=12, pady=(2,8))

        tk.Button(np_top, text="Start ALL Nodes", command=self.start_all_nodes,
                  bg=THEME["ACCENT"], fg="white", bd=0, padx=8, pady=6
        ).pack(side="left")

        tk.Button(np_top, text="Stop ALL Nodes", command=self.stop_all_nodes,
                  bg="#b44a4a", fg="white", bd=0, padx=8, pady=6
        ).pack(side="left", padx=(8,0))

        # Node list w/ scroll
        container = tk.Frame(node_card, bg=THEME["CARD"])
        container.pack(fill="both", expand=True, padx=12, pady=(6,12))

        canvas = tk.Canvas(container, bg=THEME["CARD"], highlightthickness=0)
        scroll = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
        inner = tk.Frame(canvas, bg=THEME["CARD"])

        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0,0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scroll.set)

        canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        # Create node rows
        self.node_panel_rows = {}
        for p in NODE_PORTS:
            r = tk.Frame(inner, bg=THEME["CARD"])
            r.pack(fill="x", pady=6)

            c = tk.Canvas(r, width=12, height=12, bg=THEME["CARD"], highlightthickness=0)
            o = c.create_oval(2,2,10,10, fill="#6b6b6b")
            c.pack(side="left", padx=(6,10))

            tk.Label(r, text=f"Node {p}", bg=THEME["CARD"], fg=THEME["TEXT"]).pack(side="left")

            info = tk.Label(r, text="uptime: 0s   blocks: 0   last: —",
                            bg=THEME["CARD"], fg=THEME["MUTED"])
            info.pack(side="left", padx=(8,10))

            b_stop = tk.Button(r, text="Stop", command=lambda pp=p: self.stop_node(pp),
                               bg="#b23b3b", fg="white", bd=0, padx=8)
            b_stop.pack(side="right", padx=(6,6))

            b_start = tk.Button(r, text="Start", command=lambda pp=p: self.start_node(pp),
                                bg=THEME["ACCENT"], fg="white", bd=0, padx=8)
            b_start.pack(side="right", padx=(0,6))

            self.node_panel_rows[p] = {"canvas": c, "oval": o, "info": info}

        # ---------- Activity Log ----------
        tk.Label(main, text="Activity Log", fg=THEME["TEXT"], bg=THEME["BG"],
                 font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(8,0))

        self.logbox = tk.Text(main, height=10, bg=THEME["TEXTBOX_BG"],
                              fg=THEME["TEXT"], bd=1, relief="solid")
        self.logbox.pack(fill="x", pady=(6,12))

    # ---------------- Stats Card ----------------
    def _stat_card(self, parent, title, value):
        f = tk.Frame(parent, bg=THEME["ACCENT"], width=200, height=70)
        f.pack_propagate(False)
        tk.Label(f, text=title, fg="white", bg=THEME["ACCENT"]).pack(anchor="w", padx=10, pady=(6,0))
        tk.Label(f, text=value, fg="white", bg=THEME["ACCENT"],
                 font=("Segoe UI", 18, "bold")).pack(anchor="w", padx=10)
        return f

    # ---------------- Logging ----------------
    def log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.logbox.insert(tk.END, f"[{ts}] {msg}\n")
        self.logbox.see(tk.END)

    # ---------------- Master Control ----------------
    def start_master_if_needed(self):
        try:
            requests.get(MASTER_URL + "/status", timeout=1)
        except:
            self.start_master()

    def start_master(self):
        try:
            self.master_process = subprocess.Popen(["python", "master.py"])
            self.log("Master started.")
        except Exception as e:
            self.log(f"Failed to start master: {e}")

    def stop_master(self):
        if self.master_process:
            self.master_process.terminate()
            self.master_process = None
            self.log("Master stopped.")
        else:
            try:
                requests.post(MASTER_URL + "/shutdown", timeout=1)
                self.log("Master stop requested.")
            except:
                pass

    # ---------------- Node Control ----------------
    def start_node(self, port):
        if port in self.node_processes:
            return
        try:
            proc = subprocess.Popen(["python", "node.py", port])
            self.node_processes[port] = proc
            self.log(f"Started node {port}.")
        except Exception as e:
            self.log(f"Failed to start node {port}: {e}")

    def stop_node(self, port):
        try:
            requests.post(f"http://127.0.0.1:{port}/shutdown", timeout=1)
        except:
            pass

        if port in self.node_processes:
            try:
                self.node_processes[port].terminate()
            except:
                pass
            del self.node_processes[port]

        self.log(f"Stopped node {port}.")

    def start_all_nodes(self):
        for p in NODE_PORTS:
            self.start_node(p)
        self.log("All nodes started.")

    def stop_all_nodes(self):
        for p in NODE_PORTS:
            self.stop_node(p)
        self.log("All nodes stopped.")

    # ---------------- FINAL MERGED POLL LOOP ----------------
    def _poll_loop(self):
        """
        Merged + fixed:
        - Updates Active Clients
        - Updates Failed Nodes
        - Updates Node Status Colors
        """
        while self.polling:
            try:
                resp = requests.get(MASTER_URL + "/status", timeout=2)

                if resp.status_code == 200:
                    data = resp.json()

                    nodes = data.get("nodes", {})
                    active_clients = data.get("active_clients", 0)

                    # Update active clients card
                    self.card_active_clients.children["!label2"].config(text=str(active_clients))

                    # Count failed nodes
                    failed = sum(1 for s in nodes.values() if s == "DOWN")
                    self.card_failed_nodes.children["!label2"].config(text=str(failed))

                    # Update node indicators
                    for port, row in self.node_panel_rows.items():
                        st = nodes.get(port, "DOWN")
                        color = "#53e89b" if st == "UP" else "#ff7b7b"
                        row["canvas"].itemconfig(row["oval"], fill=color)

                        ts = time.strftime("%H:%M:%S") if st == "UP" else "—"
                        row["info"].config(text=f"uptime: 0s   blocks: 0   last: {ts}")

            except Exception:
                # master unreachable → show everything down
                for row in self.node_panel_rows.values():
                    row["canvas"].itemconfig(row["oval"], fill="#6b6b6b")
                    row["info"].config(text="uptime: 0s   blocks: 0   last: —")

                self.card_active_clients.children["!label2"].config(text="0")
                self.card_failed_nodes.children["!label2"].config(text=str(len(NODE_PORTS)))

            time.sleep(POLL_INTERVAL)

    # ---------------- Client Heartbeat ----------------
    def _client_heartbeat_loop(self):
        while True:
            try:
                requests.post(MASTER_URL + "/client_heartbeat",
                              json={"id": "gui"}, timeout=1)
            except:
                pass
            time.sleep(3)

    # ---------------- Upload Helpers ----------------
    def _split_file_into_blocks(self, path):
        with open(path, "rb") as f:
            data = f.read()
        return len(data), [
            data[i:i+BLOCK_SIZE].decode("utf-8", errors="ignore")
            for i in range(0, len(data), BLOCK_SIZE)
        ]

    # ---------------- Upload ----------------
    def open_upload_dialog(self):
        path = filedialog.askopenfilename()
        if not path:
            return
        self.chosen_label.config(text=os.path.basename(path))

        rep = simpledialog.askinteger("Replication",
            f"Enter replication factor (1-{len(NODE_PORTS)}):",
            minvalue=1, maxvalue=len(NODE_PORTS)
        )
        if not rep:
            rep = 1

        threading.Thread(target=self._upload_job, args=(path, rep), daemon=True).start()

    def _upload_job(self, path, rep):
        filename = os.path.basename(path)
        self.log(f"Uploading {filename}...")

        try:
            size, blocks = self._split_file_into_blocks(path)
            num_blocks = len(blocks)

            r = requests.post(
                MASTER_URL + "/upload",
                json={"filename": filename,
                      "replication_factor": rep,
                      "num_blocks": num_blocks,
                      "size": size},
                timeout=10
            )
            if r.status_code != 200:
                GlassModal(self.root, "Upload Failed", r.text)
                return

            meta = r.json()
            block_metas = meta.get("blocks", [])

            total_steps = len(block_metas)
            self.progress["maximum"] = total_steps
            self.progress["value"] = 0

            step = 0
            for block_data, bmeta in zip(blocks, block_metas):
                block_id = bmeta["id"]
                for p in bmeta["nodes"]:
                    try:
                        requests.post(
                            f"http://127.0.0.1:{p}/block_store",
                            json={"block_id": block_id, "data": block_data},
                            timeout=8
                        )
                    except:
                        self.log(f"Node {p} failed for block {block_id}")

                step += 1
                self.progress["value"] = step

            GlassModal(self.root, "Upload Complete", f"{filename} uploaded.")
            self.log(f"Uploaded {filename}")

        except Exception as e:
            GlassModal(self.root, "Upload Error", str(e))

    # ---------------- List Files ----------------
    def list_files(self):
        try:
            r = requests.get(MASTER_URL + "/list", timeout=6)
            if r.status_code != 200:
                GlassModal(self.root, "List Failed", r.text)
                return

            lines = []
            for f, meta in r.json().items():
                lines.append(
                    f"{f} → blocks={meta['num_blocks']}, RF={meta['replication_factor']}, size={meta['size']}"
                )

            GlassModal(self.root, "Files", "\n".join(lines) or "(No files)")

        except Exception as e:
            GlassModal(self.root, "List Error", str(e))

    # ---------------- Download Popup ----------------
    def download_dialog(self):
        try:
            r = requests.get(MASTER_URL + "/list", timeout=6)
            files = list(r.json().keys())

            if not files:
                GlassModal(self.root, "No Files", "No files available to download.")
                return

            dlg = tk.Toplevel(self.root)
            dlg.title("Download File")
            dlg.geometry("460x360")
            dlg.configure(bg=THEME["CARD"])

            tk.Label(dlg, text="Files available:", fg=THEME["TEXT"],
                     bg=THEME["CARD"], font=("Segoe UI", 11, "bold")
            ).pack(anchor="w", padx=12, pady=(10,6))

            container = tk.Frame(dlg, bg=THEME["CARD"])
            container.pack(fill="both", expand=True, padx=12, pady=(0,12))

            canvas = tk.Canvas(container, bg=THEME["CARD"], highlightthickness=0)
            sbar = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
            inner = tk.Frame(canvas, bg=THEME["CARD"])

            inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.create_window((0,0), window=inner, anchor="nw")
            canvas.configure(yscrollcommand=sbar.set)

            canvas.pack(side="left", fill="both", expand=True)
            sbar.pack(side="right", fill="y")

            for f in files:
                row = tk.Frame(inner, bg=THEME["CARD"])
                row.pack(fill="x", pady=6)

                tk.Label(row, text=f, bg=THEME["CARD"], fg=THEME["TEXT"]).pack(side="left", padx=(6,4))

                tk.Button(
                    row, text="Download", bg=THEME["ACCENT"], fg="white", bd=0, padx=10,
                    command=lambda fn=f, d=dlg: (d.destroy(), threading.Thread(target=self._download_job, args=(fn,), daemon=True).start())
                ).pack(side="right", padx=(4,8))

        except Exception as e:
            GlassModal(self.root, "Error", str(e))

    def _download_job(self, filename):
        try:
            r = requests.post(MASTER_URL + "/locate",
                              json={"filename": filename}, timeout=8)

            if r.status_code != 200:
                GlassModal(self.root, "Download Failed", r.text)
                return

            assembled = []
            for blk in r.json().get("blocks", []):
                block_id = blk["id"]
                for p in blk["nodes"]:
                    try:
                        rr = requests.post(
                            f"http://127.0.0.1:{p}/block_fetch",
                            json={"block_id": block_id}, timeout=8)
                        if rr.status_code == 200:
                            assembled.append(rr.json()["data"])
                            break
                    except:
                        continue

            os.makedirs("downloads", exist_ok=True)
            outpath = os.path.join("downloads", filename)
            with open(outpath, "w", encoding="utf-8", errors="ignore") as f:
                f.write("".join(assembled))

            GlassModal(self.root, "Download Complete", f"Saved to {outpath}")
            self.log(f"Downloaded {filename} → {outpath}")

        except Exception as e:
            GlassModal(self.root, "Download Error", str(e))

    # ---------------- Delete Popup ----------------
    def delete_dialog(self):
        try:
            r = requests.get(MASTER_URL + "/list", timeout=6)
            files = list(r.json().keys())

            if not files:
                GlassModal(self.root, "No Files", "No files to delete.")
                return

            dlg = tk.Toplevel(self.root)
            dlg.title("Delete File")
            dlg.geometry("460x360")
            dlg.configure(bg=THEME["CARD"])

            tk.Label(dlg, text="Files available:", fg=THEME["TEXT"],
                     bg=THEME["CARD"], font=("Segoe UI", 11, "bold")
            ).pack(anchor="w", padx=12, pady=(10,6))

            container = tk.Frame(dlg, bg=THEME["CARD"])
            container.pack(fill="both", expand=True, padx=12, pady=(0,12))

            canvas = tk.Canvas(container, bg=THEME["CARD"], highlightthickness=0)
            sbar = tk.Scrollbar(container, orient="vertical", command=canvas.yview)
            inner = tk.Frame(canvas, bg=THEME["CARD"])

            inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.create_window((0,0), window=inner, anchor="nw")
            canvas.configure(yscrollcommand=sbar.set)

            canvas.pack(side="left", fill="both", expand=True)
            sbar.pack(side="right", fill="y")

            for f in files:
                row = tk.Frame(inner, bg=THEME["CARD"])
                row.pack(fill="x", pady=6)

                tk.Label(row, text=f, bg=THEME["CARD"], fg=THEME["TEXT"]).pack(side="left", padx=(6,4))

                tk.Button(
                    row, text="Delete", bg="#b23b3b", fg="white", bd=0, padx=10,
                    command=lambda fn=f, d=dlg: (d.destroy(), threading.Thread(target=self._delete_job, args=(fn,), daemon=True).start())
                ).pack(side="right", padx=(4,8))

        except Exception as e:
            GlassModal(self.root, "Error", str(e))

    def _delete_job(self, filename):
        try:
            r = requests.post(MASTER_URL + "/delete",
                              json={"filename": filename}, timeout=6)
            if r.status_code == 200:
                GlassModal(self.root, "Delete Success", f"Deleted {filename}")
                self.log(f"Deleted {filename}")
            else:
                GlassModal(self.root, "Delete Failed", r.text)
        except Exception as e:
            GlassModal(self.root, "Delete Error", str(e))

    # ---------------- Cleanup ----------------
    def cleanup(self):
        self.polling = False
        if self.master_process:
            try:
                self.master_process.terminate()
            except:
                pass
        for p in self.node_processes.values():
            try:
                p.terminate()
            except:
                pass


# ---------------- RUN ----------------
if __name__ == "__main__":
    root = tk.Tk()
    NeoFS_GUI(root)
    root.mainloop()
