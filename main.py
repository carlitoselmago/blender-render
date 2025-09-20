import os
import re
import subprocess
import threading
import time
from pathlib import Path
import shutil
import sys
import platform
import json
import flet as ft
import flet_dropzone as ftd   # pip install flet-dropzone

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

def render_chunk(blender_exe, blend_path, start, end, render_dir, script_name, log_cb, stop_flag, progress_cb):
    render_dir = Path(render_dir)
    render_dir.mkdir(parents=True, exist_ok=True)

    args = [str(blender_exe), "-b", str(blend_path)]
    if script_name:
        args += ["--enable-autoexec", "--python-text", script_name]
    args += [
        "-s", str(start),
        "-e", str(end),
        "-o", str(render_dir / "####"),
        "-x", "1",
        "-a",
    ]

    log_cb(f"[CMD] {' '.join(args)}")

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

    saved_re = re.compile(r"Saved:\s+'.*?[\\/](\d+)\.\w+'", re.IGNORECASE)

    while True:
        if stop_flag.is_set():
            proc.terminate()
            return
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if not line:
            continue
        log_cb(line.strip())
        m_saved = saved_re.search(line)
        if m_saved:
            try:
                cur_done = int(m_saved.group(1))
                progress_cb(cur_done)
            except ValueError:
                pass
    code = proc.wait()
    if code != 0 and not stop_flag.is_set():
        log_cb(f"[ERROR] Blender exited with {code}")

# =========================
# Worker
# =========================

class RenderWorker(threading.Thread):
    def __init__(self, page, blender_exe, files, out_root, chunk_size, run_script, script_name,
                 stop_flag, log_fn, grid_cb, progress_cb):
        super().__init__(daemon=True)
        self.page = page
        self.blender_exe = blender_exe
        self.files = [Path(f) for f in files]
        self.out_root = Path(out_root)
        self.chunk_size = int(chunk_size)
        self.run_script = run_script
        self.script_name = script_name or ""
        self.stop_flag = stop_flag
        self.log_fn = log_fn
        self.grid_cb = grid_cb
        self.progress_cb = progress_cb

    def run(self):
        for blend_path in self.files:
            if self.stop_flag.is_set():
                return
            if not blend_path.exists():
                self.log_fn(f"[WARN] File not found {blend_path}")
                continue

            render_dir = self.out_root / blend_path.stem
            try:
                s_start, s_end = get_blend_frame_range(self.blender_exe, blend_path)
            except Exception as e:
                self.log_fn(f"[ERROR] {e}")
                continue

            existing = get_existing_frames(render_dir)
            self.page.run_thread(lambda: self.grid_cb(s_start, s_end, existing))

            all_frames = list(range(s_start, s_end+1))
            missing = [f for f in all_frames if f not in existing]
            if not missing:
                self.log_fn("No missing frames.")
                continue
            ranges = list(contiguous_ranges(sorted(missing)))
            chunked = list(split_ranges_by_chunk(ranges, self.chunk_size))

            for a, b in chunked:
                if self.stop_flag.is_set():
                    return
                render_chunk(
                    self.blender_exe,
                    blend_path,
                    a, b,
                    render_dir,
                    self.script_name if self.run_script else "",
                    self.log_fn,
                    self.stop_flag,
                    lambda f: self.page.run_thread(lambda: self.progress_cb(f))
                )

# =========================
# Flet UI
# =========================

def main(page: ft.Page):
    page.title = "Blender Render (Flet Desktop)"
    page.scroll = "auto"
    page.window.height = 730

    # ---------- Load settings ----------
    def load_settings():
        if CONFIG_PATH.exists():
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_settings(*_):
        try:
            data = {
                "blender_exe": blender_path.value.strip(),
                "out_root": out_root.value.strip(),
                "chunk_size": chunk_size.value.strip(),
                "script_name": script_name.value.strip(),
                "run_script": run_script.value,
            }
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[WARN] Could not save settings: {e}")

    settings = load_settings()

    blender_path = ft.TextField(
        label="Blender executable",
        value=settings.get("blender_exe", find_default_blender()),
        width=500,
        on_change=save_settings,
    )
    out_root = ft.TextField(
        label="Output root",
        value=settings.get("out_root", str((Path.cwd()/ "RENDERSC").resolve())),
        width=500,
        on_change=save_settings,
    )
    chunk_size = ft.TextField(
        label="Chunk size",
        value=settings.get("chunk_size", "100"),
        width=100,
        input_filter=ft.NumbersOnlyInputFilter(),
        on_change=save_settings,
    )
    run_script = ft.Checkbox(
        label="Run text script at start of each chunk",
        value=settings.get("run_script", True),
        on_change=save_settings,
    )
    script_name = ft.TextField(
        label="Script name",
        value=settings.get("script_name", "lightningsync"),
        width=200,
        on_change=save_settings,
    )

    file_list = ft.ListView(expand=1, height=120, spacing=5)
    log_box = ft.ListView(expand=1, spacing=2, height=200, auto_scroll=True)
    elapsed_label = ft.Text("Elapsed: 00:00:00 | Remaining: --:--:--")

    # Grid state
    frame_squares = []
    grid = ft.GridView(expand=1, runs_count=40, max_extent=20, spacing=1, run_spacing=1, height=200)

    start_time = [None]
    stop_flag = threading.Event()
    worker = [None]
    scene_start = [None]
    scene_end = [None]

    # --- For better time estimates (session-based) ---
    existing_set_current = set()
    missing_set_current = set()
    rendered_now = set()
    total_to_render = [0]

    # ---------------- Log buffer ----------------
    log_buffer = []
    MAX_LOG_LINES = 500   # <-- only keep last 500 lines in UI

    def log_fn(msg):
        log_buffer.append(msg)

    def flush_logs():
        while True:
            if log_buffer:
                lines = list(log_buffer)
                log_buffer.clear()
                # append new lines
                for line in lines:
                    log_box.controls.append(ft.Text(line))
                # trim if too long
                if len(log_box.controls) > MAX_LOG_LINES:
                    excess = len(log_box.controls) - MAX_LOG_LINES
                    del log_box.controls[0:excess]
                page.run_thread(page.update)
            time.sleep(0.2)

    threading.Thread(target=flush_logs, daemon=True).start()

    # ---------------- Grid init ----------------
    def grid_cb(start, end, existing):
        nonlocal existing_set_current, missing_set_current
        frame_squares.clear()
        grid.controls.clear()
        scene_start[0], scene_end[0] = start, end

        existing_set_current = set(existing)
        full = set(range(start, end + 1))
        missing_set_current = full - existing_set_current
        rendered_now.clear()
        total_to_render[0] = len(missing_set_current)

        # draw grid (existing in green, missing in grey)
        for f in range(start, end+1):
            sq = ft.Container(
                width=12,
                height=12,
                bgcolor=ft.Colors.GREEN if f in existing_set_current else ft.Colors.GREY_300,
                border=ft.border.all(1, ft.Colors.BLACK),
                data=f
            )
            frame_squares.append(sq)
            grid.controls.append(sq)

        page.update()
        start_time[0] = time.time()

    # ---------------- Progress ----------------
    def progress_cb(f):
        if scene_start[0] is None:
            return
        idx = f - scene_start[0]
        if 0 <= idx < len(frame_squares):
            frame_squares[idx].bgcolor = ft.Colors.GREEN
            frame_squares[idx].update()

        if f in missing_set_current:
            rendered_now.add(f)

        elapsed = time.time() - (start_time[0] or time.time())
        done_now = len(rendered_now)
        remain_frames = max(0, total_to_render[0] - done_now)

        if done_now > 0:
            avg_per_frame = elapsed / done_now
            remaining = avg_per_frame * remain_frames
        else:
            remaining = 0

        def fmt(sec):
            h, rem = divmod(int(sec), 3600)
            m, s = divmod(rem, 60)
            return f"{h:02}:{m:02}:{s:02}"

        elapsed_label.value = f"Elapsed: {fmt(elapsed)} | Remaining: {fmt(remaining) if done_now>0 else '--:--:--'}"
        elapsed_label.update()

    # ---------------- File pickers ----------------
    def on_files_result(e: ft.FilePickerResultEvent):
        if e.files:
            for f in e.files:
                if f.path.endswith(".blend"):
                    file_list.controls.append(ft.Text(f.path))
            page.update()
            save_settings()

    def on_exe_result(e: ft.FilePickerResultEvent):
        if e.files:
            blender_path.value = e.files[0].path
            page.update()
            save_settings()

    def on_dir_result(e: ft.FilePickerResultEvent):
        if e.path:
            out_root.value = e.path
            page.update()
            save_settings()

    file_picker = ft.FilePicker(on_result=on_files_result)
    exe_picker = ft.FilePicker(on_result=on_exe_result)
    dir_picker = ft.FilePicker(on_result=on_dir_result)
    page.overlay.extend([file_picker, exe_picker, dir_picker])

    def add_files(e):
        file_picker.pick_files(allow_multiple=True)

    def browse_exe(e):
        exe_picker.pick_files(allow_multiple=False)

    def browse_out(e):
        dir_picker.get_directory_path()

    # ---------------- Dropzone handler ----------------
    def handle_drop(e):
        if e.files:
            for f in e.files:
                if f.endswith(".blend"):
                    file_list.controls.append(ft.Text(f))
            page.update()
            save_settings()

    file_dropzone = ftd.Dropzone(
        content=ft.Container(
            content=file_list,
            bgcolor=ft.Colors.with_opacity(0.15, ft.Colors.WHITE),
            border=ft.border.all(1, ft.Colors.GREY),
            border_radius=5,
            padding=10,
            expand=True,
        ),
        on_dropped=handle_drop
    )

    def remove_selected(e):
        if file_list.controls:
            file_list.controls.pop()
            page.update()
            save_settings()

    def clear_list(e):
        file_list.controls.clear()
        page.update()
        save_settings()

    # ---------------- Controls ----------------
    def start_render(e):
        if worker[0] and worker[0].is_alive():
            log_fn("Already running.")
            return
        files = [c.value for c in file_list.controls]
        if not files:
            log_fn("No files selected.")
            return
        stop_flag.clear()
        log_box.controls.clear()
        worker[0] = RenderWorker(
            page,
            blender_path.value,
            files,
            out_root.value,
            chunk_size.value,
            run_script.value,
            script_name.value,
            stop_flag,
            log_fn,
            grid_cb,
            progress_cb
        )
        worker[0].start()
        log_fn("Started render.")

    def stop_render(e):
        if worker[0] and worker[0].is_alive():
            stop_flag.set()
            log_fn("Stop requested.")
        else:
            log_fn("Nothing running.")

    # ---------------- Layout ----------------
    page.add(
        ft.Column([
            ft.Row([blender_path, ft.ElevatedButton("Browse…", on_click=browse_exe)]),
            ft.Row([out_root, ft.ElevatedButton("Browse…", on_click=browse_out)]),
            ft.Row([chunk_size, run_script, script_name]),
            ft.Row([
                ft.ElevatedButton("Add Files…", on_click=add_files),
                ft.ElevatedButton("Remove Selected", on_click=remove_selected),
                ft.ElevatedButton("Clear List", on_click=clear_list),
            ]),
            ft.Text("Files:"),
            file_dropzone,
            ft.Text("Frames:"),
            grid,
            ft.Row(
                [
                    elapsed_label,
                    ft.Row(
                        [
                            ft.ElevatedButton("Start", on_click=start_render),
                            ft.ElevatedButton("Stop", on_click=stop_render),
                        ]
                    ),
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            ),
            ft.Text("Log:"),
            log_box,
        ], scroll="auto")
    )

# Run
ft.app(target=main)
