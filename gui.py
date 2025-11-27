import tkinter as tk
from tkinter import filedialog, simpledialog
import threading
import client


# ---------- Helper Thread ----------
def run_in_thread(func, *args):
    threading.Thread(target=func, args=args).start()


# ---------- GUI Application ----------
class DFS_GUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Distributed File System - GUI (Dark Mode)")
        self.root.geometry("650x550")

        # DARK THEME COLORS
        self.bg = "#1b1b1b"        # background
        self.fg = "#ffffff"        # text
        self.btn_bg = "#2e2e2e"    # button background
        self.btn_fg = "#00aaff"    # blue text
        self.log_bg = "#0f0f0f"    # log background

        self.root.configure(bg=self.bg)

        # Title
        tk.Label(
            root,
            text="Distributed File System",
            font=("Arial", 22, "bold"),
            fg=self.btn_fg,
            bg=self.bg
        ).pack(pady=15)

        # Buttons Frame
        frame = tk.Frame(root, bg=self.bg)
        frame.pack(pady=5)

        self.make_btn(frame, "Upload File", self.upload_file, 0, 0)
        self.make_btn(frame, "Download File", self.download_file, 0, 1)
        self.make_btn(frame, "List Files", self.list_files, 1, 0)
        self.make_btn(frame, "Delete File", self.delete_file, 1, 1)
        self.make_btn(frame, "Node Status", self.node_status, 2, 0, 2)

        # Output Log
        self.output = tk.Text(
            root,
            height=14,
            width=70,
            bg=self.log_bg,
            fg=self.fg,
            insertbackground=self.fg,
            bd=2,
            relief="ridge"
        )
        self.output.pack(pady=20)

    # ---------- Button Creator ----------
    def make_btn(self, frame, text, command, row, col, colspan=1):
        tk.Button(
            frame,
            text=text,
            width=20,
            font=("Arial", 11, "bold"),
            command=command,
            bg=self.btn_bg,
            fg=self.btn_fg,
            activebackground="#3c3c3c",
            activeforeground="#00ccff",
            relief="ridge",
            bd=2
        ).grid(row=row, column=col, columnspan=colspan, padx=10, pady=10)

    # ---------- Button Actions ----------
    def upload_file(self):
        filepath = filedialog.askopenfilename()
        if filepath:
            run_in_thread(self._upload, filepath)

    def _upload(self, filepath):
        try:
            resp = client.upload_file(filepath)
            self.log(f"[UPLOAD]\n{filepath}\n{resp}")
        except Exception as e:
            self.log(f"[ERROR] Upload failed: {e}")

    def download_file(self):
        filename = simpledialog.askstring("Download", "Enter filename:")
        if filename:
            run_in_thread(self._download, filename)

    def _download(self, filename):
        try:
            resp = client.download_file(filename)
            self.log(f"[DOWNLOAD]\n{filename}\nSaved at: {resp}")
        except Exception as e:
            self.log(f"[ERROR] Download failed: {e}")

    def list_files(self):
        run_in_thread(self._list)

    def _list(self):
        try:
            data = client.list_files()
            self.log("📄 Files in DFS:\n" + "\n".join(data))
        except Exception as e:
            self.log(f"[ERROR] Could not list files: {e}")

    def delete_file(self):
        filename = simpledialog.askstring("Delete", "Enter filename:")
        if filename:
            run_in_thread(self._delete, filename)

    def _delete(self, filename):
        try:
            resp = client.delete_file(filename)
            self.log(f"[DELETE]\n{filename}\n{resp}")
        except Exception as e:
            self.log(f"[ERROR] Delete failed: {e}")

    def node_status(self):
        run_in_thread(self._status)

    def _status(self):
        try:
            status = client.get_node_status()
            pretty = "\n".join([f"Node {n['id']} → {n['status']}" for n in status])
            self.log("🔌 Node Status:\n" + pretty)
        except Exception as e:
            self.log(f"[ERROR] Status check failed: {e}")

    # ---------- Logger ----------
    def log(self, text):
        self.output.insert(tk.END, text + "\n\n")
        self.output.see(tk.END)


# ---------- Start GUI ----------
if __name__ == "__main__":
    root = tk.Tk()
    app = DFS_GUI(root)
    root.mainloop()
