"""
NeoFS Glassmorphic GUI
Palette:
 - BG: #EFECE3 (light cream)
 - CARD: #8FABD4 (light blue)
 - ACCENT: #4A70A9 (mid blue)
 - TEXT: #000000 (black)
Modal style: centered, rounded, white glow border
Active Clients replaces Replicas
"""

import tkinter as tk
from tkinter import ttk, filedialog, simpledialog
import threading, requests, subprocess, time, os, uuid, atexit

MASTER_URL = "http://127.0.0.1:4000"
NODE_PORTS = ["5001", "5002", "5003", "5004", "5005"]
POLL_INTERVAL = 1.0

# Colors from palette
BG = "#EFECE3"
CARD = "#8FABD4"
ACCENT = "#4A70A9"
TEXT = "#000000"
MODAL_BG = "#FFFFFF"
MODAL_FG = "#000000"

class GlassModal(tk.Toplevel):
    def __init__(self, parent, title, message, confirm_text="OK", cancel_text=None, on_confirm=None):
        super().__init__(parent)
        self.transient(parent)
        self.grab_set()
        self.title(title)
        self.configure(bg=MODAL_BG)
        self.resizable(False, False)

        w, h = 440, 180
        self.geometry(f"{w}x{h}")

        parent.update_idletasks()
        px = parent.winfo_rootx()
        py = parent.winfo_rooty()
        pw = parent.winfo_width()
        ph = parent.winfo_height()
        x = px + (pw - w)//2
        y = py + (ph - h)//2
        self.geometry(f"+{max(20,x)}+{max(20,y)}")

        outer = tk.Frame(self, bg=ACCENT, bd=0)
        outer.place(x=6, y=6, relwidth=1, relheight=1)
        inner = tk.Frame(outer, bg=MODAL_BG, bd=0)
        inner.pack(expand=True, fill="both", padx=6, pady=6)

        tk.Label(inner, text=title, font=("Segoe UI", 12, "bold"), bg=MODAL_BG, fg=MODAL_FG).pack(anchor="w", padx=12, pady=(8,2))
        tk.Label(inner, text=message, font=("Segoe UI", 10), bg=MODAL_BG, fg=MODAL_FG, wraplength=400, justify="left").pack(padx=12, pady=(0,12))

        btnf = tk.Frame(inner, bg=MODAL_BG)
        btnf.pack(side="bottom", anchor="e", padx=12, pady=8)

        if cancel_text:
            tk.Button(btnf, text=cancel_text, command=self._cancel, bg=ACCENT, fg="white", bd=0).pack(side="right", padx=6)

        tk.Button(btnf, text=confirm_text, command=self._confirm, bg=ACCENT, fg="white", bd=0).pack(side="right", padx=6)
        self.on_confirm = on_confirm

    def _confirm(self):
        if self.on_confirm:
            try: self.on_confirm()
            except: pass
        self.destroy()

    def _cancel(self):
        self.destroy()


class NeoFS_GUI:
    def __init__(self, root):
        self.root = root
        root.title("NeoFS - Distributed File System")
        root.geometry("1120x760")
        root.minsize(980, 640)
        root.configure(bg=BG)

        self.client_id = str(uuid.uuid4())
        self.master_process = None
        self.node_processes = {}
        self.stat_vars = {
            "total": tk.StringVar(value=str(len(NODE_PORTS))),
            "active_clients": tk.StringVar(value="1"),
            "failed": tk.StringVar(value="0")
        }
        self.node_widgets = {}

        self._build_ui()
        self.start_master_if_needed()

        self.polling = True
        threading.Thread(target=self._poll_loop, daemon=True).start()
        threading.Thread(target=self._client_heartbeat_loop, daemon=True).start()

        atexit.register(self.cleanup)


    # ================= UI ====================

    def _build_ui(self):

        sidebar = tk.Frame(self.root, bg=ACCENT, width=260)
        sidebar.pack(side="left", fill="y")
        tk.Label(sidebar, text="NeoFS", font=("Segoe UI", 20, "bold"), fg="white", bg=ACCENT).pack(pady=(18,6))
        tk.Label(sidebar, text="Distributed File System", fg="white", bg=ACCENT).pack()

        menu = [
            ("Upload File", self.open_upload_dialog),
            ("Download File", self.download_dialog),
            ("List Files", self.list_files),
            ("Delete File", self.delete_dialog),
            ("Node Panel", self.node_panel)
        ]

        for t, c in menu:
            tk.Button(sidebar, text=t, command=c, bg=CARD, fg=TEXT, bd=0, pady=8).pack(fill="x", padx=12, pady=8)

        tk.Label(sidebar, text="Node Status", fg="white", bg=ACCENT, font=("Segoe UI", 10, "bold")).pack(pady=(12,4))

        for p in NODE_PORTS:
            row = tk.Frame(sidebar, bg=ACCENT)
            row.pack(fill="x", padx=8, pady=4)
            c = tk.Canvas(row, width=14, height=14, bg=ACCENT, highlightthickness=0)
            oid = c.create_oval(2,2,12,12, fill="#ff6b6b")
            c.pack(side="left", padx=(4,8))
            lbl = tk.Label(row, text=f"Node {p}", bg=ACCENT, fg="white")
            lbl.pack(side="left")
            ts = tk.Label(row, text="—", bg=ACCENT, fg="white")
            ts.pack(side="right")
            self.node_widgets[p] = {"canvas": c, "oval": oid, "ts": ts}

        tk.Button(sidebar, text="Start Master", command=self.start_master, bg="#163b52", fg="white", bd=0).pack(fill="x", padx=12, pady=(18,4))
        tk.Button(sidebar, text="Stop Master", command=self.stop_master, bg="#7a3a3a", fg="white", bd=0).pack(fill="x", padx=12)


        main = tk.Frame(self.root, bg=BG)
        main.pack(side="left", fill="both", expand=True, padx=18, pady=12)

        hdr = tk.Frame(main, bg=BG)
        hdr.pack(fill="x", pady=(6,14))

        self._stat_card(hdr, "Total Nodes", self.stat_vars["total"]).pack(side="left", padx=8)
        self._stat_card(hdr, "Active Clients", self.stat_vars["active_clients"]).pack(side="left", padx=8)
        self._stat_card(hdr, "Failed Nodes", self.stat_vars["failed"]).pack(side="left", padx=8)

        body = tk.Frame(main, bg=BG)
        body.pack(fill="both", expand=True)

        leftcol = tk.Frame(body, bg=BG)
        leftcol.pack(side="left", fill="y", padx=(0,12))

        upcard = tk.Frame(leftcol, bg=CARD, width=420, height=240, bd=0)
        upcard.pack(pady=6)
        upcard.pack_propagate(False)

        tk.Label(upcard, text="File Upload", bg=CARD, fg=TEXT, font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=12, pady=(8,0))
        
        self.drop = tk.Frame(upcard, bg="#ffffff", width=380, height=130)
        self.drop.pack(padx=12, pady=8)
        self.drop.pack_propagate(False)

        tk.Label(self.drop, text="Click to select file or drag here", bg="#ffffff", fg=TEXT).pack(expand=True)

        # 🔥 CLICKABLE DROP ZONE
        self.drop.bind("<Button-1>", lambda e: self.open_upload_dialog())

        self.chosen_label = tk.Label(upcard, text="", bg=CARD, fg=TEXT)
        self.chosen_label.pack()

        self.progress = ttk.Progressbar(upcard, orient="horizontal", length=360, mode="determinate")
        self.progress.pack(pady=8)


        rightcol = tk.Frame(body, bg=BG)
        rightcol.pack(side="left", fill="both", expand=True)

        tk.Label(rightcol, text="System Details", bg=BG, fg=TEXT, font=("Segoe UI", 12, "bold")).pack(anchor="w")
        self.details = tk.Text(rightcol, height=14, bg="#fbfbfe", fg=TEXT)
        self.details.pack(fill="both", expand=True, pady=8)

        tk.Label(rightcol, text="Activity Log", bg=BG, fg=TEXT, font=("Segoe UI", 12, "bold")).pack(anchor="w")
        self.logbox = tk.Text(rightcol, height=6, bg="#fbfbfe", fg=TEXT)
        self.logbox.pack(fill="x")


    # ----------- FUNCTIONS BELOW REMAIN SAME -----------

    def _stat_card(self, parent, title, var):
        f = tk.Frame(parent, bg=ACCENT, width=180, height=72)
        f.pack_propagate(False)
        tk.Label(f, text=title, bg=ACCENT, fg="white").pack(anchor="w", padx=10, pady=(6,0))
        tk.Label(f, textvariable=var, bg=ACCENT, fg="white", font=("Segoe UI", 18, "bold")).pack(anchor="w", padx=10)
        return f

    def log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.logbox.insert(tk.END, f"[{ts}] {msg}\n")
        self.logbox.see(tk.END)
        self.details.insert(tk.END, f"[{ts}] {msg}\n")
        self.details.see(tk.END)

    def start_master_if_needed(self):
        try:
            requests.get(MASTER_URL + "/status", timeout=1)
            self.log("Master already running.")
        except:
            self.start_master()

    def start_master(self):
        try:
            self.master_process = subprocess.Popen(["python", "master.py"])
            self.log("Master started by GUI.")
        except Exception as e:
            self.log(f"Failed to start master: {e}")

    def stop_master(self):
        if self.master_process:
            try:
                self.master_process.terminate()
                self.log("Master terminated (GUI requested).")
            except Exception as e:
                self.log(f"Error stopping master: {e}")
            self.master_process = None
        else:
            self.log("Master not started by GUI.")

    def node_panel(self):
        GlassModal(self.root, "Node Panel", "Start or stop nodes from this panel", confirm_text="Close")
        w = tk.Toplevel(self.root)
        w.title("Node Panel")
        w.geometry("420x360")
        w.configure(bg=BG)
        tk.Label(w, text="Start / Stop Nodes", bg=BG, fg=TEXT, font=("Segoe UI", 12, "bold")).pack(pady=10)

        for p in NODE_PORTS:
            row = tk.Frame(w, bg=BG)
            row.pack(fill="x", padx=12, pady=6)
            tk.Label(row, text=f"Node {p}", bg=BG, fg=TEXT, width=12, anchor="w").pack(side="left")
            tk.Button(row, text="Start", bg=ACCENT, fg="white", command=lambda pp=p: self.start_node(pp)).pack(side="left", padx=6)
            tk.Button(row, text="Stop", bg="#b44a4a", fg="white", command=lambda pp=p: self.stop_node(pp)).pack(side="left", padx=6)


    def start_node(self, port):
        if port in self.node_processes:
            self.log(f"Node {port} already started.")
            return
        try:
            p = subprocess.Popen(["python", "node.py", port])
            self.node_processes[port] = p
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
        self.log(f"Stop requested for node {port}.")

    def _poll_loop(self):
        while True:
            try:
                resp = requests.get(MASTER_URL + "/status", timeout=2)
                if resp.status_code == 200:
                    data = resp.json()
                    nodes = data.get("nodes", {})
                    ac = data.get("active_clients", 0)
                    self.stat_vars["active_clients"].set(str(ac))

                    for p, w in self.node_widgets.items():
                        st = nodes.get(p, "DOWN")
                        color = "#53e89b" if st == "UP" else "#ff7b7b"
                        w["canvas"].itemconfig(w["oval"], fill=color)
                        w["ts"].config(text=time.strftime("%H:%M:%S") if st == "UP" else "—")

                    failed = len([1 for p in NODE_PORTS if nodes.get(p, "DOWN") != "UP"])
                    self.stat_vars["failed"].set(str(failed))

                else:
                    for p, w in self.node_widgets.items():
                        w["canvas"].itemconfig(w["oval"], fill="#6b6b6b")

                time.sleep(POLL_INTERVAL)

            except:
                for p, w in self.node_widgets.items():
                    w["canvas"].itemconfig(w["oval"], fill="#6b6b6b")
                    w["ts"].config(text="—")
                time.sleep(POLL_INTERVAL)

    def _client_heartbeat_loop(self):
        while True:
            try:
                requests.post(MASTER_URL + "/client_heartbeat", json={"id": self.client_id}, timeout=1)
            except:
                pass
            time.sleep(3)

    def open_upload_dialog(self):
        path = filedialog.askopenfilename()
        if not path:
            return
        self.chosen_label.config(text=os.path.basename(path))
        rep = simpledialog.askinteger("Replication", "Enter replication factor (1-5):", minvalue=1, maxvalue=len(NODE_PORTS))
        if not rep:
            rep = 1
        threading.Thread(target=self._upload_job, args=(path, rep), daemon=True).start()

    def _upload_job(self, path, rep):
        filename = os.path.basename(path)
        self.log(f"Requesting master to store {filename} (RF={rep})")
        try:
            r = requests.post(MASTER_URL + "/upload", json={"filename": filename, "replication_factor": rep}, timeout=6)
            if r.status_code != 200:
                self.log(f"Master error: {r.text}")
                GlassModal(self.root, "Upload Failed", f"Master error: {r.text}", confirm_text="OK")
                return
            nodes = r.json().get("store_in", [])

            with open(path, "rb") as f:
                content = f.read().decode("utf-8", errors="ignore")

            total = len(nodes)
            self.progress["maximum"] = total * 100
            self.progress["value"] = 0

            for i, p in enumerate(nodes):
                self.log(f"Pushing {filename} -> node {p}")
                try:
                    rr = requests.post(f"http://127.0.0.1:{p}/store", json={"filename": filename, "data": content}, timeout=12)
                    if rr.status_code == 200:
                        self.log(f"Stored on node {p}")
                    else:
                        self.log(f"Node {p} store failed: {rr.text}")
                except Exception as e:
                    self.log(f"Error pushing to node {p}: {e}")

                for step in range(0, 101, 20):
                    self.progress["value"] = i * 100 + step
                    self.root.update_idletasks()
                    time.sleep(0.04)

                self.progress["value"] = (i + 1) * 100

            self.progress["value"] = 0
            GlassModal(self.root, "Upload Complete", f"{filename} uploaded to {nodes}", confirm_text="OK")

        except Exception as e:
            self.log(f"Upload exception: {e}")
            GlassModal(self.root, "Upload Error", str(e), confirm_text="OK")


    def download_dialog(self):
        def on_confirm():
            fname = ent.get().strip()
            if fname:
                threading.Thread(target=self._download_job, args=(fname,), daemon=True).start()
                dlg.destroy()

        dlg = tk.Toplevel(self.root)
        dlg.title("Download File")
        dlg.geometry("420x140")
        dlg.configure(bg=MODAL_BG)
        tk.Label(dlg, text="Enter filename to download", bg=MODAL_BG).pack(pady=(10,6))
        ent = tk.Entry(dlg, width=40)
        ent.pack(pady=6)
        tk.Button(dlg, text="Download", command=on_confirm, bg=ACCENT, fg="white").pack(pady=6)

    def _download_job(self, filename):
        try:
            r = requests.post(MASTER_URL + "/locate", json={"filename": filename}, timeout=6)
            if r.status_code != 200:
                self.log(f"Locate failed: {r.text}")
                GlassModal(self.root, "Download Failed", f"Locate failed: {r.text}", confirm_text="OK")
                return
            nodes = r.json().get("nodes", [])

            for p in nodes:
                try:
                    rr = requests.post(f"http://127.0.0.1:{p}/download", json={"filename": filename}, timeout=8)
                    if rr.status_code == 200:
                        data = rr.json().get("data", "")
                        os.makedirs("downloads", exist_ok=True)
                        with open(os.path.join("downloads", filename), "w", encoding="utf-8", errors="ignore") as f:
                            f.write(data)
                        self.log(f"Downloaded {filename} from node {p}")
                        GlassModal(self.root, "Download Complete", f"Saved to downloads/{filename}", confirm_text="OK")
                        return
                except Exception as e:
                    self.log(f"Node {p} failed: {e}")

            GlassModal(self.root, "Download Failed", "Failed to download from all replicas.", confirm_text="OK")

        except Exception as e:
            self.log(f"Download exception: {e}")
            GlassModal(self.root, "Download Error", str(e), confirm_text="OK")


    def list_files(self):
        try:
            r = requests.get(MASTER_URL + "/list", timeout=6)
            if r.status_code == 200:
                d = r.json()
                GlassModal(self.root, "Files in DFS", "\n".join([f"{k} -> {v}" for k,v in d.items()]) or "(no files)", confirm_text="Close")
            else:
                GlassModal(self.root, "List Failed", r.text, confirm_text="OK")
        except Exception as e:
            GlassModal(self.root, "List Error", str(e), confirm_text="OK")


    def delete_dialog(self):
        def on_confirm():
            fname = ent.get().strip()
            if fname:
                threading.Thread(target=self._delete_job, args=(fname,), daemon=True).start()
                dlg.destroy()

        dlg = tk.Toplevel(self.root)
        dlg.title("Delete File")
        dlg.geometry("420x140")
        dlg.configure(bg=MODAL_BG)
        tk.Label(dlg, text="Enter filename to delete", bg=MODAL_BG).pack(pady=(10,6))
        ent = tk.Entry(dlg, width=40)
        ent.pack(pady=6)
        tk.Button(dlg, text="Delete", command=on_confirm, bg="#b23b3b", fg="white").pack(pady=6)


    def _delete_job(self, filename):
        try:
            r = requests.post(MASTER_URL + "/delete", json={"filename": filename}, timeout=6)
            if r.status_code == 200:
                self.log(f"Deleted {filename} -> {r.json()}")
                GlassModal(self.root, "Delete Success", f"Deleted {filename}", confirm_text="OK")
            else:
                self.log(f"Delete failed: {r.text}")
                GlassModal(self.root, "Delete Failed", r.text, confirm_text="OK")
        except Exception as e:
            self.log(f"Delete exception: {e}")
            GlassModal(self.root, "Delete Error", str(e), confirm_text="OK")


    def cleanup(self):
        self.polling = False
        if self.master_process:
            try: self.master_process.terminate()
            except: pass

        for p in list(self.node_processes.values()):
            try: p.terminate()
            except: pass


if __name__ == "__main__":
    root = tk.Tk()
    app = NeoFS_GUI(root)
    root.mainloop()
