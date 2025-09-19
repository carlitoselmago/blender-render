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
    os.environ["TKDND_LIBRARY"] = str(Path(_MEI) / "tkdnd")

# Drag & Drop
DND_AVAILABLE = False
DND_FILES = None
BaseTk = tk.Tk

try:
    import tkinterdnd2 as tkdnd2
    from tkinterdnd2 import DND_FILES as _DND_FILES, TkinterDnD

    if not _MEI and not os.environ.get("TKDND_LIBRARY"):
        pkg_lib = Path(tkdnd2.__file__).parent / "tkdnd"
        if pkg_lib.exists():
            os.environ["TKDND_LIBRARY"] = str(pkg_lib)

    DND_FILES = _DND_FILES
    BaseTk = TkinterDnD.Tk
    DND_AVAILABLE = True
except Exception:
    DND_AVAILABLE = False

CONFIG_PATH = Path(__file__).with_name(".blender_render_gui.json")

# =========================
# Utils
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
        raise RuntimeError(f"Could not parse frame range for {blend_path}.\n{out}")
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

    try:
        while True:
            if stop_flag and stop_flag.is_set():
                terminate_proc_tree(proc, log_cb)
                return
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            if not line:
                continue
            m_saved = saved_re.search(line)
            if m_saved and progress_cb and scene_start is not None and scene_end is not None:
                try:
                    cur_done = int(m_saved.group(1))
                    progress_cb(cur_done, scene_start, scene_end)
                except ValueError:
                    pass
        code = proc.wait()
        if code != 0 and not (stop_flag and stop_flag.is_set()):
            raise RuntimeError(f"Blender exited with code {code} for chunk {start}-{end}.")
    finally:
        if current_proc_holder is not None:
            current_proc_holder["proc"] = None

# =========================
# Worker
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
        self.log_queue.put(msg + "\n")

    def set_progress(self, current_file, scene_start, scene_end, current_frame):
        self.progress_queue.put((str(current_file), scene_start, scene_end, current_frame))

    def stop_immediately(self):
        self.stop_flag.set()
        proc = self.current_proc_holder.get("proc")
        if proc:
            terminate_proc_tree(proc, self.log)

    def run(self):
        try:
            for blend_path in self.files:
                if self.stop_flag.is_set():
                    break
                if not blend_path.exists():
                    continue
                base = blend_path.stem
                render_dir = self.out_root / base
                try:
                    s_start, s_end = get_blend_frame_range(self.blender_exe, blend_path)
                except Exception as e:
                    self.log(f"[ERROR] {e}")
                    continue
                existing = get_existing_frames(render_dir)
                self.progress_queue.put(("INITGRID", s_start, s_end, existing))
                all_frames = list(range(s_start, s_end + 1))
                missing_frames = [f for f in all_frames if f not in existing]
                if not missing_frames:
                    self.finished_naturally = True
                    return
                ranges = list(contiguous_ranges(sorted(missing_frames)))
                chunked_ranges = list(split_ranges_by_chunk(ranges, self.chunk_size))
                for (a, b) in chunked_ranges:
                    if self.stop_flag.is_set():
                        break
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
                        self.log(f"[ERROR] {e}")
                        continue
            self.finished_naturally = True
        except Exception as e:
            self.log(f"[FATAL] {e}")

# =========================
# Tkinter GUI
# =========================

class App(BaseTk):
    def __init__(self):
        super().__init__()
        self.title("Blender render")
        self.geometry("980x820")
        self.files = []
        self.worker = None
        self.stop_flag = threading.Event()
        self.log_queue = queue.Queue()
        self.progress_queue = queue.Queue()
        self._finish_handled = False
        self.render_start_time = None
        self.frame_rects = {}
        self.scene_start = None
        self.scene_end = None

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Blender executable").grid(row=0, column=0, sticky="w")
        self.blender_var = tk.StringVar(value=find_default_blender())
        self.blender_entry = ttk.Entry(frm, textvariable=self.blender_var, width=80)
        self.blender_entry.grid(row=0, column=1, sticky="we", padx=8)

        ttk.Label(frm, text="Output root").grid(row=1, column=0, sticky="w")
        self.out_var = tk.StringVar(value=str((Path.cwd()/ "RENDERSC").resolve()))
        self.out_entry = ttk.Entry(frm, textvariable=self.out_var, width=80)
        self.out_entry.grid(row=1, column=1, sticky="we", padx=8)

        ttk.Label(frm, text="Chunk size").grid(row=2, column=0, sticky="w")
        self.chunk_var = tk.StringVar(value="100")
        self.chunk_entry = ttk.Entry(frm, textvariable=self.chunk_var, width=10)
        self.chunk_entry.grid(row=2, column=1, sticky="w", padx=8)

        files_bar = ttk.Frame(frm)
        files_bar.grid(row=5, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(files_bar, text="Add Files…", command=self.add_files).pack(side="left")

        self.listbox = tk.Listbox(frm, selectmode=tk.EXTENDED, height=5)
        self.listbox.grid(row=6, column=0, columnspan=3, sticky="nsew")

        bar = ttk.Frame(frm)
        bar.grid(row=7, column=0, columnspan=3, sticky="we", pady=(10, 4))
        ttk.Button(bar, text="Start", command=self.start_render).pack(side="left")
        ttk.Button(bar, text="Stop", command=self.stop_render).pack(side="left", padx=6)

        self.current_label = ttk.Label(frm, text="Now Rendering: -")
        self.current_label.grid(row=8, column=0, columnspan=3, sticky="w")

        self.frames_canvas = tk.Canvas(frm, height=200, bg="white")
        self.frames_canvas.grid(row=9, column=0, columnspan=3, sticky="we")
        self.frame_label = ttk.Label(frm, text="- / -")
        self.frame_label.grid(row=10, column=0, columnspan=2, sticky="w")
        self.time_label = ttk.Label(frm, text="Elapsed: 00:00:00 | Remaining: --:--:--")
        self.time_label.grid(row=10, column=2, sticky="e")

        self.log = tk.Text(frm, height=12)
        self.log.grid(row=11, column=0, columnspan=3, sticky="nsew")

        self.after(100, self.drain_queues)

    def init_frame_grid(self, start, end, existing):
        self.frames_canvas.delete("all")
        self.frame_rects.clear()
        total = end - start + 1
        cols = 40
        size = 12
        pad = 2
        rows = (total + cols - 1) // cols
        self.frames_canvas.config(height=rows * (size + pad) + pad)
        for i, frame in enumerate(range(start, end+1)):
            r = i // cols
            c = i % cols
            x0 = c*(size+pad) + pad
            y0 = r*(size+pad) + pad
            rect = self.frames_canvas.create_rectangle(x0, y0, x0+size, y0+size,
                                                       fill="green" if frame in existing else "lightgray",
                                                       outline="black")
            self.frame_rects[frame] = rect
        self.scene_start, self.scene_end = start, end

    def update_frame_done(self, frame):
        rect = self.frame_rects.get(frame)
        if rect:
            self.frames_canvas.itemconfig(rect, fill="green")

    def add_files(self):
        paths = filedialog.askopenfilenames(title="Select .blend files", filetypes=[("Blender files", "*.blend")])
        for p in paths:
            if p not in self.files:
                self.files.append(p)
        self.refresh_listbox()

    def refresh_listbox(self):
        self.listbox.delete(0, tk.END)
        for f in self.files:
            self.listbox.insert(tk.END, f)

    def start_render(self):
        if self.worker and self.worker.is_alive():
            return
        blender_exe = self.blender_var.get().strip()
        out_root = self.out_var.get().strip()
        chunk_size = int(self.chunk_var.get().strip())
        self.stop_flag.clear()
        self.render_start_time = time.time()
        self.worker = RenderWorker(
            blender_exe=blender_exe,
            files=self.files,
            out_root=out_root,
            chunk_size=chunk_size,
            run_script=False,
            script_name="",
            log_queue=self.log_queue,
            progress_queue=self.progress_queue,
            stop_flag=self.stop_flag
        )
        self.worker.start()

    def stop_render(self):
        if self.worker and self.worker.is_alive():
            self.worker.stop_immediately()

    def drain_queues(self):
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log.insert(tk.END, msg)
        except queue.Empty:
            pass
        try:
            while True:
                current_file, s_start, s_end, cur = self.progress_queue.get_nowait()
                if current_file == "INITGRID":
                    self.init_frame_grid(s_start, s_end, cur)
                else:
                    self.current_label.config(text=f"Now Rendering: {current_file}")
                    self.frame_label.config(text=f"{cur} / {s_end}")
                    self.update_frame_done(cur)
        except queue.Empty:
            pass
        self.after(100, self.drain_queues)

# =========================
# Entry
# =========================

if __name__ == "__main__":
    app = App()
    app.mainloop()
