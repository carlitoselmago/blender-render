import os
import re
import subprocess
import threading
import time
from pathlib import Path
import queue
import shutil

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# Drag & Drop (requires: pip install tkinterdnd2)
try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
    DND_AVAILABLE = True
except Exception:
    DND_AVAILABLE = False  # App still runs; only Add Files… works.


# =========================
# Utils (defaults & shutdown)
# =========================

def find_default_blender():
    """Return a sensible default blender executable from PATH."""
    for cand in ("blender", "blender.exe"):
        p = shutil.which(cand)
        if p:
            return p
    return "blender.exe" if os.name == "nt" else "blender"

def kill_all_blender(log_fn=None):
    """Force-close all Blender instances (used on app exit)."""
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/IM", "blender.exe", "/T", "/F"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
            )
        else:
            subprocess.run(["pkill", "-f", "blender"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            subprocess.run(["killall", "-q", "blender"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    except Exception as e:
        if log_fn:
            log_fn(f"[WARN] Failed to kill Blender: {e}\n")


# =========================
# Render + scan helpers
# =========================

def run_capture(cmd):
    """Run command, capture combined stdout+stderr as text (utf-8 with replacement)."""
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )
    return proc.returncode, proc.stdout or ""

def get_blend_frame_range(blender_exe, blend_path):
    """Query Blender for active scene frame range."""
    cmd = [
        str(blender_exe), "-b", str(blend_path),
        "--python-expr",
        "import bpy; s=bpy.context.scene; print(f'RANGE {s.frame_start} {s.frame_end}')"
    ]
    code, out = run_capture(cmd)
    if code != 0:
        raise RuntimeError(f"Failed to read frame range for {blend_path} (code {code}).\n{out}")

    m = re.search(r'^RANGE\s+(\d+)\s+(\d+)\s*$', out, flags=re.MULTILINE)
    if not m:
        time.sleep(0.3)
        code2, out2 = run_capture(cmd)
        if code2 != 0:
            raise RuntimeError(f"Failed to read frame range for {blend_path} (code {code2}).\n{out2}")
        m = re.search(r'^RANGE\s+(\d+)\s+(\d+)\s*$', out2, flags=re.MULTILINE)
        if not m:
            raise RuntimeError(f"Could not parse frame range for {blend_path}.\n{out}\n{out2}")
    return int(m.group(1)), int(m.group(2))

def get_existing_frames(render_dir: Path):
    """Return a set of frame numbers present in render_dir by parsing trailing digits in filenames."""
    frames = set()
    if not render_dir.exists():
        return frames
    for p in render_dir.iterdir():
        if p.is_file():
            m = re.search(r'(\d+)$', p.stem) or re.search(r'(\d+)', p.stem)
            if m:
                try:
                    frames.add(int(m.group(1)))
                except ValueError:
                    pass
    return frames

def contiguous_ranges(sorted_frames):
    """Given a sorted list of integers, yield (start, end) contiguous ranges."""
    if not sorted_frames:
        return
    start = prev = sorted_frames[0]
    for x in sorted_frames[1:]:
        if x == prev + 1:
            prev = x
            continue
        yield (start, prev)
        start = prev = x
    yield (start, prev)

def split_ranges_by_chunk(ranges, chunk_size):
    """Split (start,end) ranges into chunks of at most chunk_size."""
    for a, b in ranges:
        cur = a
        while cur <= b:
            yield (cur, min(cur + chunk_size - 1, b))
            cur += chunk_size

def format_ranges(ranges, max_show=10):
    """Human-readable ranges, e.g., '100–120, 127, 130–135' (limit for log neatness)."""
    parts = []
    for i, (a, b) in enumerate(ranges):
        if i >= max_show:
            parts.append("…")
            break
        parts.append(str(a) if a == b else f"{a}–{b}")
    return ", ".join(parts)

def render_chunk(
    blender_exe,
    blend_path,
    start,
    end,
    render_dir,
    run_script=True,
    script_name="lightningsync",
    log_cb=None,
    stop_flag=None,
    scene_start=None,
    scene_end=None,
    progress_cb=None,
):
    """Render frames [start..end] as a fresh Blender process, streaming logs and per-frame completion."""
    render_dir = Path(render_dir)
    render_dir.mkdir(parents=True, exist_ok=True)

    # IMPORTANT: load the .blend FIRST, then the text block (so Blender can find it)
    args = [str(blender_exe), "-b", str(blend_path)]
    #args = [str(blender_exe),  str(blend_path)]
    if run_script and script_name.strip():
        args += ["--enable-autoexec", "--python-text", script_name.strip()]
    args += [
        "-s", str(start),
        "-e", str(end),
        "-o", str(render_dir / "####"),
        "-x", "1",
        "-a",
    ]

    if log_cb:
        log_cb(f"[CMD] {' '.join(args)}\n")

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )

    saved_re = re.compile(r"Saved:\s+'.*?[\\/](\d+)\.\w+'", re.IGNORECASE)
    last_completed = None

    while True:
        if stop_flag and stop_flag.is_set():
            pass

        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if not line:
            continue

        if log_cb:
            log_cb(line)

        m_saved = saved_re.search(line)
        if m_saved and progress_cb and scene_start is not None and scene_end is not None:
            try:
                cur_done = int(m_saved.group(1))
                if start <= cur_done <= end:
                    last_completed = cur_done
                    progress_cb(cur_done, scene_start, scene_end)
            except ValueError:
                pass

    code = proc.wait()
    if code != 0:
        raise RuntimeError(f"Blender exited with code {code} for chunk {start}-{end}.")

    if progress_cb and last_completed is None:
        progress_cb(end, scene_start, scene_end)


# =========================
# Worker thread (renders only missing frames)
# =========================

class RenderWorker(threading.Thread):
    def __init__(self, blender_exe, files, out_root, chunk_size, run_script, script_name,
                 log_queue, progress_queue, stop_flag):
        super().__init__(daemon=True)
        self.blender_exe = blender_exe
        self.files = [Path(f) for f in files]
        self.out_root = Path(out_root)
        self.chunk_size = int(chunk_size)
        self.run_script = bool(run_script)
        self.script_name = script_name or ""
        self.log_queue = log_queue
        self.progress_queue = progress_queue
        self.stop_flag = stop_flag

    def log(self, msg):
        self.log_queue.put(msg if msg.endswith("\n") else msg + "\n")

    def set_progress(self, current_file, scene_start, scene_end, current_frame):
        self.progress_queue.put((str(current_file), scene_start, scene_end, current_frame))

    def run(self):
        try:
            for blend_path in self.files:
                if self.stop_flag.is_set():
                    self.log("[STOP] Stopping before starting next file.")
                    break

                if not blend_path.exists():
                    self.log(f"[WARN] File not found, skipping: {blend_path}")
                    continue

                base = blend_path.stem
                render_dir = self.out_root / base
                self.log(f"\n=== {blend_path.name} ===")
                self.log(f"Output: {render_dir}")

                # Scene frame range
                try:
                    s_start, s_end = get_blend_frame_range(self.blender_exe, blend_path)
                except Exception as e:
                    self.log(f"[ERROR] {e}")
                    continue

                self.set_progress(blend_path.name, s_start, s_end, s_start)
                self.log(f"Scene frames: {s_start}..{s_end}")

                # Determine missing frames
                existing = get_existing_frames(render_dir)
                all_frames = list(range(s_start, s_end + 1))
                missing_frames = [f for f in all_frames if f not in existing]

                if not missing_frames:
                    self.log("No missing frames. Skipping.")
                    self.set_progress(blend_path.name, s_start, s_end, s_end)
                    continue

                missing_frames.sort()
                ranges = list(contiguous_ranges(missing_frames))
                self.log(f"Missing frames: {len(missing_frames)}")
                self.log(f"Ranges: {format_ranges(ranges)}")

                # Split ranges by chunk size
                chunked_ranges = list(split_ranges_by_chunk(ranges, self.chunk_size))
                self.log(f"Planned chunks: {format_ranges(chunked_ranges, max_show=20)}")

                # Start progress at first missing frame
                self.set_progress(blend_path.name, s_start, s_end, missing_frames[0])

                # Render each chunk of missing frames
                for (a, b) in chunked_ranges:
                    if self.stop_flag.is_set():
                        self.log("[STOP] Stop requested. Halting mid-file.")
                        return
                    self.log(f"Rendering missing chunk: {a}..{b}")
                    try:
                        render_chunk(
                            blender_exe=self.blender_exe,
                            blend_path=blend_path,
                            start=a,
                            end=b,
                            render_dir=render_dir,
                            run_script=self.run_script,
                            script_name=self.script_name,
                            log_cb=self.log,
                            stop_flag=self.stop_flag,
                            scene_start=s_start,
                            scene_end=s_end,
                            progress_cb=lambda cur_frame, _s, _e: self.set_progress(blend_path.name, s_start, s_end, cur_frame),
                        )
                    except Exception as e:
                        self.log(f"[ERROR] {e}")
                        # continue to next chunk (maybe one frame was bad)
                        continue

                    # Safety: ensure we land on the end of the chunk
                    self.set_progress(blend_path.name, s_start, s_end, b)

                self.log(f"Finished (or halted) for {blend_path.name}.")
            self.log("\nAll done (or stopped).")
        except Exception as e:
            self.log(f"[FATAL] {e}")


# =========================
# Tkinter GUI (with optional DnD)
# =========================

class App((TkinterDnD.Tk if DND_AVAILABLE else tk.Tk)):
    def __init__(self):
        super().__init__()
        self.title("Blender Chunk Renderer (Missing-Frames Mode)")
        self.geometry("930x780")

        # State
        self.files = []
        self.worker = None
        self.stop_flag = threading.Event()
        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()

        # Close handler: stop worker and kill Blender
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # Top controls
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        # Blender exe (prefilled)
        ttk.Label(frm, text="Blender executable").grid(row=0, column=0, sticky="w")
        self.blender_var = tk.StringVar(value=find_default_blender())
        self.blender_entry = ttk.Entry(frm, textvariable=self.blender_var, width=80)
        self.blender_entry.grid(row=0, column=1, sticky="we", padx=8)
        ttk.Button(frm, text="Browse…", command=self.pick_blender).grid(row=0, column=2, sticky="w")

        # Output root
        ttk.Label(frm, text="Output root").grid(row=1, column=0, sticky="w")
        self.out_var = tk.StringVar(value="G:\\RENDERSC" if os.name == "nt" else str((Path.cwd()/ "RENDERSC").resolve()))
        self.out_entry = ttk.Entry(frm, textvariable=self.out_var, width=80)
        self.out_entry.grid(row=1, column=1, sticky="we", padx=8)
        ttk.Button(frm, text="Browse…", command=self.pick_out_root).grid(row=1, column=2, sticky="w")

        # Chunk size
        ttk.Label(frm, text="Chunk size").grid(row=2, column=0, sticky="w")
        self.chunk_var = tk.StringVar(value="100")
        self.chunk_entry = ttk.Entry(frm, textvariable=self.chunk_var, width=10)
        self.chunk_entry.grid(row=2, column=1, sticky="w", padx=8)

        # Run script (optional)
        self.run_script_var = tk.BooleanVar(value=True)
        self.script_name_var = tk.StringVar(value="lightningsync")

        run_frame = ttk.Frame(frm)
        run_frame.grid(row=3, column=0, columnspan=3, sticky="we", pady=(6, 4))
        run_chk = ttk.Checkbutton(run_frame, text="Run text script at start of each chunk", variable=self.run_script_var, command=self.toggle_script_field)
        run_chk.pack(side="left")
        ttk.Label(run_frame, text="Script name:").pack(side="left", padx=(12, 6))
        self.script_entry = ttk.Entry(run_frame, textvariable=self.script_name_var, width=24)
        self.script_entry.pack(side="left")

        # Files list + buttons
        files_bar = ttk.Frame(frm)
        files_bar.grid(row=4, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(files_bar, text="Add Files…", command=self.add_files).pack(side="left")
        ttk.Button(files_bar, text="Remove Selected", command=self.remove_selected).pack(side="left", padx=6)
        ttk.Button(files_bar, text="Clear List", command=self.clear_list).pack(side="left")

        self.listbox = tk.Listbox(frm, selectmode=tk.EXTENDED, height=10)
        self.listbox.grid(row=5, column=0, columnspan=3, sticky="nsew")
        frm.rowconfigure(5, weight=1)
        frm.columnconfigure(1, weight=1)

        # Enable DnD on listbox (and whole window) if available
        if DND_AVAILABLE:
            try:
                self.listbox.drop_target_register(DND_FILES)
                self.listbox.dnd_bind('<<Drop>>', self.on_drop)
                self.drop_target_register(DND_FILES)
                self.dnd_bind('<<Drop>>', self.on_drop)
            except Exception:
                pass

        # Start/Stop
        bar = ttk.Frame(frm)
        bar.grid(row=6, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(bar, text="Start", command=self.start_render).pack(side="left")
        ttk.Button(bar, text="Stop", command=self.stop_render).pack(side="left", padx=6)

        # Progress
        self.current_label = ttk.Label(frm, text="Now Rendering: -")
        self.current_label.grid(row=7, column=0, columnspan=3, sticky="w", pady=(10, 2))

        self.progress = ttk.Progressbar(frm, orient="horizontal", length=400, mode="determinate")
        self.progress.grid(row=8, column=0, columnspan=3, sticky="we")
        self.frame_label = ttk.Label(frm, text="- / -")
        self.frame_label.grid(row=9, column=0, columnspan=3, sticky="w")

        # Log
        self.log = tk.Text(frm, height=12)
        self.log.grid(row=10, column=0, columnspan=3, sticky="nsew", pady=(10,0))
        frm.rowconfigure(10, weight=1)

        # Status line re: DnD
        status = ttk.Label(
            frm,
            #text=("Drag & drop: enabled" if DND_AVAILABLE else "Drag & drop: install 'tkinterdnd2' to enable"),
            text=(""),
            foreground=("#10b981" if DND_AVAILABLE else "#b45309")
        )
        status.grid(row=11, column=0, columnspan=3, sticky="w", pady=(6,0))

        # Initialize script field state
        self.toggle_script_field()

        # Poll queues
        self.after(100, self.drain_queues)

    # -------- Close app --------
    def on_close(self):
        try:
            self.stop_flag.set()
            if self.worker and self.worker.is_alive():
                self.log_insert("Closing: killing Blender processes…\n")
        except Exception:
            pass
        kill_all_blender(self.log_insert)
        try:
            self.destroy()
        except Exception:
            os._exit(0)

    # -------- Script field enable/disable --------
    def toggle_script_field(self):
        if self.run_script_var.get():
            self.script_entry.configure(state="normal")
        else:
            self.script_entry.configure(state="disabled")

    # ------------- DnD helpers -------------

    @staticmethod
    def _parse_dropped_paths(data: str):
        """Convert a DND_FILES string to a list of absolute paths."""
        paths = []
        token = ""
        in_brace = False
        for ch in data:
            if ch == "{":
                in_brace = True
                token = ""
            elif ch == "}":
                in_brace = False
                if token:
                    paths.append(token)
                    token = ""
            elif ch == " " and not in_brace:
                if token:
                    paths.append(token)
                    token = ""
            else:
                token += ch
        if token:
            paths.append(token)

        out = []
        for p in paths:
            p = p.strip().strip('"')
            if p.lower().endswith(".blend"):
                out.append(p)
        return out

    def on_drop(self, event):
        try:
            dropped = self._parse_dropped_paths(event.data or "")
            added = 0
            for p in dropped:
                if p not in self.files:
                    self.files.append(p)
                    added += 1
            if added:
                self.refresh_listbox()
                self.log_insert(f"[DnD] Added {added} file(s).\n")
        except Exception as e:
            self.log_insert(f"[DnD ERROR] {e}\n")

    # ------------- UI Callbacks -------------

    def pick_blender(self):
        path = filedialog.askopenfilename(
            title="Select Blender executable",
            filetypes=[("Executable", "*.exe;*")] if os.name == "nt" else [("All files","*.*")]
        )
        if path:
            self.blender_var.set(path)

    def pick_out_root(self):
        path = filedialog.askdirectory(title="Select output root")
        if path:
            self.out_var.set(path)

    def add_files(self):
        paths = filedialog.askopenfilenames(
            title="Select .blend files",
            filetypes=[("Blender files", "*.blend")]
        )
        added = 0
        for p in paths:
            if p.lower().endswith(".blend") and p not in self.files:
                self.files.append(p)
                added += 1
        if added:
            self.refresh_listbox()

    def remove_selected(self):
        sel = list(self.listbox.curselection())
        sel.sort(reverse=True)
        for idx in sel:
            del self.files[idx]
        self.refresh_listbox()

    def clear_list(self):
        self.files.clear()
        self.refresh_listbox()

    def refresh_listbox(self):
        self.listbox.delete(0, tk.END)
        for f in self.files:
            self.listbox.insert(tk.END, f)

    def start_render(self):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Info", "Rendering already in progress.")
            return

        blender_exe = self.blender_var.get().strip()
        out_root = self.out_var.get().strip()
        chunk = self.chunk_var.get().strip()
        run_script = self.run_script_var.get()
        script_name = self.script_name_var.get().strip()

        if not blender_exe:
            messagebox.showerror("Error", "Set Blender executable.")
            return
        if not self.files:
            messagebox.showerror("Error", "Add at least one .blend file.")
            return
        try:
            chunk_size = int(chunk)
            if chunk_size <= 0:
                raise ValueError
        except:
            messagebox.showerror("Error", "Chunk size must be a positive integer.")
            return

        if run_script and not script_name:
            messagebox.showerror("Error", "Provide a script name or uncheck 'Run text script'.")
            return

        self.stop_flag.clear()
        self.current_label.config(text="Now Rendering: -")
        self.progress.config(value=0, maximum=100)
        self.frame_label.config(text="- / -")
        self.log.delete(1.0, tk.END)

        self.worker = RenderWorker(
            blender_exe=blender_exe,
            files=self.files,
            out_root=out_root,
            chunk_size=chunk_size,
            run_script=run_script,
            script_name=script_name,
            log_queue=self.log_queue,
            progress_queue=self.progress_queue,
            stop_flag=self.stop_flag
        )
        self.worker.start()
        self.log_insert("Started.\n")

    def stop_render(self):
        if self.worker and self.worker.is_alive():
            self.stop_flag.set()
            self.log_insert("Stopping after current chunk…\n")
        else:
            self.log_insert("Nothing is running.\n")

    # ------------- Queue draining -------------

    def drain_queues(self):
        # Logs
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_insert(msg)
        except queue.Empty:
            pass

        # Progress tuples: (file, s_start, s_end, cur)
        try:
            while True:
                current_file, s_start, s_end, cur = self.progress_queue.get_nowait()
                self.current_label.config(text=f"Now Rendering: {current_file}")
                self.frame_label.config(text=f"{cur} / {s_end}")
                total = max(1, s_end - s_start + 1)
                done = max(0, min(total, (cur - s_start + 1)))
                pct = int(done * 100 / total)
                self.progress.config(value=pct, maximum=100)
        except queue.Empty:
            pass

        self.after(100, self.drain_queues)

    def log_insert(self, text):
        self.log.insert(tk.END, text)
        self.log.see(tk.END)


# =========================
# Entry
# =========================

if __name__ == "__main__":
    app = App()
    app.mainloop()
