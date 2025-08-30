import os
import re
import subprocess
import threading
import time
from pathlib import Path
import queue
import shutil
import sys
import platform
import json
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ---------------------------------------------------------------------
# tkdnd location (runtime + compiled)
# ---------------------------------------------------------------------
_MEI = getattr(sys, "_MEIPASS", None)
if _MEI:
    # In a frozen (PyInstaller) app, force TKDND_LIBRARY to the bundled folder
    os.environ["TKDND_LIBRARY"] = str(Path(_MEI) / "tkdnd")

# Drag & Drop (requires: pip install tkinterdnd2)
DND_AVAILABLE = False
DND_FILES = None
BaseTk = tk.Tk  # fallback

try:
    import tkinterdnd2 as tkdnd2
    from tkinterdnd2 import DND_FILES as _DND_FILES, TkinterDnD

    # For source runs, if TKDND_LIBRARY not set, point to the package's tkdnd dir
    if not _MEI and not os.environ.get("TKDND_LIBRARY"):
        pkg_lib = Path(tkdnd2.__file__).parent / "tkdnd"
        if pkg_lib.exists():
            os.environ["TKDND_LIBRARY"] = str(pkg_lib)

    DND_FILES = _DND_FILES
    BaseTk = TkinterDnD.Tk  # <-- use DnD-aware root when available
    DND_AVAILABLE = True
except Exception:
    DND_AVAILABLE = False

# -------------------------
# Config path for settings
# -------------------------
CONFIG_PATH = Path(__file__).with_name(".blender_render_gui.json")


# =========================
# Utils (defaults & shutdown)
# =========================

def find_default_blender():
    for cand in ("blender", "blender.exe"):
        p = shutil.which(cand)
        if p:
            return p
    return "blender.exe" if os.name == "nt" else "blender"

def kill_all_blender(log_fn=None):
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

def terminate_proc_tree(proc, log_fn=None):
    if not proc:
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        else:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
    except Exception as e:
        if log_fn:
            log_fn(f"[WARN] Failed to terminate process: {e}\n")

def perform_shutdown(log_fn=None):
    system = platform.system().lower()
    try:
        if "windows" in system:
            subprocess.Popen(["shutdown", "/s", "/t", "0"])
        elif "darwin" in system:
            subprocess.Popen(["osascript", "-e", 'tell application "System Events" to shut down'])
        else:
            subprocess.Popen(["systemctl", "poweroff"])
        if log_fn:
            log_fn("[INFO] Shutdown command issued.\n")
    except Exception as e:
        if log_fn:
            log_fn(f"[ERROR] Failed to initiate shutdown: {e}\n")


# =========================
# Render + scan helpers
# =========================

def run_capture(cmd):
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
    for a, b in ranges:
        cur = a
        while cur <= b:
            yield (cur, min(cur + chunk_size - 1, b))
            cur += chunk_size

def format_ranges(ranges, max_show=10):
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
    current_proc_holder=None,
):
    render_dir = Path(render_dir)
    render_dir.mkdir(parents=True, exist_ok=True)

    args = [str(blender_exe), "-b", str(blend_path)]
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
        bufsize=1
    )

    if current_proc_holder is not None:
        current_proc_holder["proc"] = proc

    saved_re = re.compile(r"Saved:\s+'.*?[\\/](\d+)\.\w+'", re.IGNORECASE)
    last_completed = None

    try:
        while True:
            if stop_flag and stop_flag.is_set():
                if log_cb:
                    log_cb("[STOP] Immediate stop requested. Killing Blender…\n")
                terminate_proc_tree(proc, log_cb)
                return

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
        if code != 0 and not (stop_flag and stop_flag.is_set()):
            raise RuntimeError(f"Blender exited with code {code} for chunk {start}-{end}.")
    finally:
        if current_proc_holder is not None:
            current_proc_holder["proc"] = None

    if progress_cb and last_completed is None:
        progress_cb(end, scene_start, scene_end)


# =========================
# Worker thread
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

        self.current_proc_holder = {"proc": None}
        self.finished_naturally = False

    def log(self, msg):
        self.log_queue.put(msg if msg.endswith("\n") else msg + "\n")

    def set_progress(self, current_file, scene_start, scene_end, current_frame):
        self.progress_queue.put((str(current_file), scene_start, scene_end, current_frame))

    def stop_immediately(self):
        self.stop_flag.set()
        proc = self.current_proc_holder.get("proc")
        if proc:
            self.log("[STOP] Killing active Blender process…")
            terminate_proc_tree(proc, self.log)

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

                try:
                    s_start, s_end = get_blend_frame_range(self.blender_exe, blend_path)
                except Exception as e:
                    self.log(f"[ERROR] {e}")
                    continue

                self.set_progress(blend_path.name, s_start, s_end, s_start)
                self.log(f"Scene frames: {s_start}..{s_end}")

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

                chunked_ranges = list(split_ranges_by_chunk(ranges, self.chunk_size))
                self.log(f"Planned chunks: {format_ranges(chunked_ranges, max_show=20)}")

                self.set_progress(blend_path.name, s_start, s_end, missing_frames[0])

                for (a, b) in chunked_ranges:
                    if self.stop_flag.is_set():
                        self.log("[STOP] Stop requested. Halting mid-file.")
                        break

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
                            current_proc_holder=self.current_proc_holder,
                        )
                    except Exception as e:
                        if self.stop_flag.is_set():
                            self.log("[STOP] Stopped during chunk.")
                            break
                        self.log(f"[ERROR] {e}")
                        continue

                    if self.stop_flag.is_set():
                        self.log("[STOP] Halting immediately after chunk stop.")
                        break

                    self.set_progress(blend_path.name, s_start, s_end, b)

                if self.stop_flag.is_set():
                    break

                self.log(f"Finished (or halted) for {blend_path.name}.")

            if not self.stop_flag.is_set():
                self.finished_naturally = True
                self.log("\nAll done.")
            else:
                self.log("\nStopped.")
        except Exception as e:
            self.log(f"[FATAL] {e}")


# =========================
# Tkinter GUI (DnD via tkinterdnd2)
# =========================

class App(BaseTk):
    def __init__(self):
        super().__init__()
        self.title("Blender render")
        self.geometry("980x820")

        # State
        self.files = []
        self.worker = None
        self.stop_flag = threading.Event()
        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self._finish_handled = False
        self._shutdown_scheduled = False
        self._save_job = None

        self.protocol("WM_DELETE_WINDOW", self.on_close)

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Blender executable").grid(row=0, column=0, sticky="w")
        self.blender_var = tk.StringVar(value=find_default_blender())
        self.blender_entry = ttk.Entry(frm, textvariable=self.blender_var, width=80)
        self.blender_entry.grid(row=0, column=1, sticky="we", padx=8)
        ttk.Button(frm, text="Browse…", command=self.pick_blender).grid(row=0, column=2, sticky="w")

        ttk.Label(frm, text="Output root").grid(row=1, column=0, sticky="w")
        self.out_var = tk.StringVar(value="G:\\RENDERSC" if os.name == "nt" else str((Path.cwd()/ "RENDERSC").resolve()))
        self.out_entry = ttk.Entry(frm, textvariable=self.out_var, width=80)
        self.out_entry.grid(row=1, column=1, sticky="we", padx=8)
        ttk.Button(frm, text="Browse…", command=self.pick_out_root).grid(row=1, column=2, sticky="w")

        ttk.Label(frm, text="Chunk size").grid(row=2, column=0, sticky="w")
        self.chunk_var = tk.StringVar(value="100")
        self.chunk_entry = ttk.Entry(frm, textvariable=self.chunk_var, width=10)
        self.chunk_entry.grid(row=2, column=1, sticky="w", padx=8)

        self.run_script_var = tk.BooleanVar(value=True)
        self.script_name_var = tk.StringVar(value="lightningsync")

        run_frame = ttk.Frame(frm)
        run_frame.grid(row=3, column=0, columnspan=3, sticky="we", pady=(6, 4))
        run_chk = ttk.Checkbutton(run_frame, text="Run text script at start of each chunk", variable=self.run_script_var, command=self.toggle_script_field)
        run_chk.pack(side="left")
        ttk.Label(run_frame, text="Script name:").pack(side="left", padx=(12, 6))
        self.script_entry = ttk.Entry(run_frame, textvariable=self.script_name_var, width=24)
        self.script_entry.pack(side="left")

        self.shutdown_var = tk.BooleanVar(value=False)
        self.shutdown_chk = ttk.Checkbutton(frm, text="Shut down computer when all files finish (60s warning)",
                                            variable=self.shutdown_var)
        self.shutdown_chk.grid(row=4, column=0, columnspan=3, sticky="w", pady=(8, 4))

        files_bar = ttk.Frame(frm)
        files_bar.grid(row=5, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(files_bar, text="Add Files…", command=self.add_files).pack(side="left")
        ttk.Button(files_bar, text="Remove Selected", command=self.remove_selected).pack(side="left", padx=6)
        ttk.Button(files_bar, text="Clear List", command=self.clear_list).pack(side="left")

        self.listbox = tk.Listbox(frm, selectmode=tk.EXTENDED, height=10)
        self.listbox.grid(row=6, column=0, columnspan=3, sticky="nsew")
        frm.rowconfigure(6, weight=1)
        frm.columnconfigure(1, weight=1)

        # ----- DnD status line
        self._dnd_status = tk.StringVar(value="")

        if DND_AVAILABLE:
            try:
                ver = self.tk.call('package', 'require', 'tkdnd')
                # Register drop targets with tkinterdnd2 helpers
                self.listbox.drop_target_register(DND_FILES)
                self.listbox.dnd_bind('<<Drop>>', self.on_drop)

                try:
                    self.drop_target_register(DND_FILES)
                    self.dnd_bind('<<Drop>>', self.on_drop)
                except Exception:
                    pass

                self._dnd_status.set(f"Drag & drop ready (tkdnd {ver}; TKDND_LIBRARY={os.environ.get('TKDND_LIBRARY','?')})")
            except Exception as e:
                self._dnd_status.set(f"Drag & drop not available: {e}")
        else:
            self._dnd_status.set("Drag & drop disabled (tkinterdnd2 not installed)")

        bar = ttk.Frame(frm)
        bar.grid(row=7, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(bar, text="Start", command=self.start_render).pack(side="left")
        ttk.Button(bar, text="Stop", command=self.stop_render).pack(side="left", padx=6)

        self.current_label = ttk.Label(frm, text="Now Rendering: -")
        self.current_label.grid(row=8, column=0, columnspan=3, sticky="w", pady=(10, 2))

        self.progress = ttk.Progressbar(frm, orient="horizontal", length=400, mode="determinate")
        self.progress.grid(row=9, column=0, columnspan=3, sticky="we")
        self.frame_label = ttk.Label(frm, text="- / -")
        self.frame_label.grid(row=10, column=0, columnspan=3, sticky="w")

        self.log = tk.Text(frm, height=12)
        self.log.grid(row=11, column=0, columnspan=3, sticky="nsew", pady=(10,0))
        frm.rowconfigure(11, weight=1)

        status = ttk.Label(frm, textvariable=self._dnd_status)
        status.grid(row=12, column=0, columnspan=3, sticky="w", pady=(6,0))

        self.load_settings()
        self.toggle_script_field()

        for var in (self.blender_var, self.out_var, self.chunk_var,
                    self.run_script_var, self.script_name_var, self.shutdown_var):
            var.trace_add("write", lambda *args: self._schedule_save())

        self.after(100, self.drain_queues)

    # -------- Settings persistence --------
    def _collect_settings(self):
        return {
            "blender_exe": self.blender_var.get().strip(),
            "out_root": self.out_var.get().strip(),
            "chunk_size": self.chunk_var.get().strip(),
            "run_script": bool(self.run_script_var.get()),
            "script_name": self.script_name_var.get().strip(),
            "shutdown_after_finish": bool(self.shutdown_var.get()),
            "geometry": self.geometry(),
        }

    def save_settings(self):
        try:
            data = self._collect_settings()
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log_insert(f"[WARN] Could not save settings: {e}\n")

    def load_settings(self):
        if not CONFIG_PATH.exists():
            return
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "blender_exe" in data: self.blender_var.set(data["blender_exe"])
            if "out_root" in data: self.out_var.set(data["out_root"])
            if "chunk_size" in data: self.chunk_var.set(str(data["chunk_size"]))
            if "run_script" in data: self.run_script_var.set(bool(data["run_script"]))
            if "script_name" in data: self.script_name_var.set(data["script_name"])
            if "shutdown_after_finish" in data: self.shutdown_var.set(bool(data["shutdown_after_finish"]))
            self.after(100, lambda: self._apply_geometry(data.get("geometry")))
        except Exception as e:
            self.log_insert(f"[WARN] Could not load settings: {e}\n")

    def _apply_geometry(self, geom):
        try:
            if geom:
                self.geometry(geom)
        except Exception:
            pass

    def _schedule_save(self, delay_ms=500):
        if getattr(self, "_save_job", None):
            try:
                self.after_cancel(self._save_job)
            except Exception:
                pass
        self._save_job = self.after(delay_ms, self.save_settings)

    # -------- Close app --------
    def on_close(self):
        try:
            self.save_settings()
        except Exception:
            pass
        try:
            self.stop_flag.set()
            if self.worker and self.worker.is_alive():
                self.log_insert("Closing: killing Blender processes…\n")
                try:
                    self.worker.stop_immediately()
                except Exception:
                    pass
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
        except Exception:
            messagebox.showerror("Error", "Chunk size must be a positive integer.")
            return

        if run_script and not script_name:
            messagebox.showerror("Error", "Provide a script name or uncheck 'Run text script'.")
            return

        self.stop_flag.clear()
        self._finish_handled = False
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
            try:
                self.worker.stop_immediately()
                self.log_insert("Stop pressed: killed active Blender instance.\n")
            except Exception as e:
                self.log_insert(f"[WARN] Could not stop immediately: {e}\n")
        else:
            self.log_insert("Nothing is running.\n")

    def schedule_shutdown(self, seconds=60):
        if self._shutdown_scheduled:
            return
        self._shutdown_scheduled = True

        top = tk.Toplevel(self)
        top.title("Render finished")
        top.attributes("-topmost", True)
        top.geometry("420x140")
        top.resizable(False, False)
        top.protocol("WM_DELETE_WINDOW", lambda: None)

        msg = ttk.Label(top, text="Rendering finished. The computer will shut down automatically.",
                        wraplength=400, justify="center")
        msg.pack(pady=(16, 8))

        countdown_var = tk.StringVar()
        lbl = ttk.Label(top, textvariable=countdown_var, font=("TkDefaultFont", 12))
        lbl.pack()

        try:
            self.update_idletasks()
            x = self.winfo_rootx() + (self.winfo_width() - 420) // 2
            y = self.winfo_rooty() + (self.winfo_height() - 140) // 2
            top.geometry(f"+{max(0,x)}+{max(0,y)}")
        except Exception:
            pass

        def tick(remaining):
            if remaining <= 0:
                try:
                    top.destroy()
                except Exception:
                    pass
                perform_shutdown(self.log_insert)
                return
            countdown_var.set(f"Shutting down in {remaining} seconds…")
            self.after(1000, lambda: tick(remaining - 1))

        tick(int(seconds))

    def drain_queues(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_insert(msg)
        except queue.Empty:
            pass

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

        if self.worker and not self.worker.is_alive() and not self._finish_handled:
            self._finish_handled = True
            if getattr(self.worker, "finished_naturally", False) and self.shutdown_var.get():
                self.schedule_shutdown(seconds=60)

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
