import os
import re
import subprocess
import threading
import time
import socket
import shutil
import sys
import json
import struct
from pathlib import Path
import flet as ft
import flet_dropzone as ftd  # pip install flet-dropzone

CONFIG_PATH = Path(__file__).with_name(".blender_render_gui.json")

# ---- Network constants ----
DISCOVERY_PORT = 50000
DISCOVERY_MAGIC = b"BLENDER_DISCOVER"
JOB_PORT_DEFAULT = 50010
UPLOAD_PORT = 50020  # main's server to receive frames from clients

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

# ---- Dependency scanner (via Blender) ----
def get_blend_dependencies(blender_exe: str, blend_path: Path):
    """
    Return list of (abs_src, remote_rel) to send to client.
    Relative assets (under blend dir) keep their relative paths.
    Absolute assets are moved under '_external/<basename>' and remapped client-side.
    """
    blend_path = Path(blend_path).resolve()
    blend_dir = str(blend_path.parent).replace("\\", "\\\\")
    py = rf"""
import bpy, os, json
def abspath(p):
    try: return os.path.normpath(bpy.path.abspath(p))
    except: return None
deps=set()
for img in bpy.data.images:
    p=getattr(img,'filepath','') or ''
    ap=abspath(p)
    if ap and not img.packed_file and os.path.isfile(ap): deps.add(ap)
for lib in bpy.data.libraries:
    ap=abspath(lib.filepath)
    if ap and os.path.isfile(ap): deps.add(ap)
for snd in bpy.data.sounds:
    p=getattr(snd,'filepath','') or ''
    ap=abspath(p)
    if ap and os.path.isfile(ap): deps.add(ap)
print('DEPS '+json.dumps(sorted(deps)))
"""
    code, out = run_capture([str(blender_exe), "-b", str(blend_path), "--python-expr", py])
    if code != 0:
        raise RuntimeError(f"Dependency scan failed for {blend_path}.\n{out}")
    m = re.search(r'^DEPS\s+(\[.*\])\s*$', out, flags=re.MULTILINE)
    deps = []
    if m:
        try:
            deps = json.loads(m.group(1))
        except Exception:
            deps = []
    result = []
    for ap in deps:
        ap = Path(ap)
        try:
            rel = ap.relative_to(blend_path.parent)
            remote_rel = str(rel).replace("\\", "/")
        except ValueError:
            remote_rel = f"_external/{ap.name}"
        result.append((str(ap), remote_rel))
    return result

# ---- Send job (TCP) to client: header + .blend + dependencies ----
def send_job(ip: str, port: int, header: dict, files: list[tuple[str, str]], log_cb):
    """
    files: list of (local_abs_path, remote_rel_path).
    Order: first the .blend (remote_rel is its filename), then dependencies in header['dependencies'] order.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((ip, port))
        hdr = json.dumps(header).encode("utf-8")
        sock.sendall(struct.pack("!I", len(hdr)))
        sock.sendall(hdr)
        for local_path, _remote_rel in files:
            size = os.path.getsize(local_path)
            sock.sendall(struct.pack("!Q", size))
            with open(local_path, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    sock.sendall(chunk)
        sock.close()
        log_cb(f"[MAIN] Sent job {header.get('job_id')} to {ip}:{port}")
    except Exception as e:
        log_cb(f"[ERROR] send_job to {ip}:{port}: {e}")

# =========================
# Local rendering helper (per chunk)
# =========================

def render_chunk(blender_exe, blend_path, start, end, render_dir, run_script, script_name, log_cb, stop_flag, progress_cb):
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

    log_cb(f"[LOCAL CMD] {' '.join(args)}")

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

# A worker that renders a list of (start,end) chunks locally
class LocalChunksWorker(threading.Thread):
    def __init__(self, blender_exe, blend_path, render_dir, chunks, run_script, script_name, stop_flag, log_cb, progress_cb):
        super().__init__(daemon=True)
        self.blender_exe = blender_exe
        self.blend_path = blend_path
        self.render_dir = render_dir
        self.chunks = chunks
        self.run_script = run_script
        self.script_name = script_name
        self.stop_flag = stop_flag
        self.log_cb = log_cb
        self.progress_cb = progress_cb

    def run(self):
        for a, b in self.chunks:
            if self.stop_flag.is_set():
                return
            render_chunk(
                self.blender_exe,
                self.blend_path,
                a, b,
                self.render_dir,
                self.run_script,
                self.script_name,
                self.log_cb,
                self.stop_flag,
                self.progress_cb
            )

# =========================
# Main upload server (receives frames from clients)
# =========================

def start_upload_server(get_render_dir_fn, progress_cb, log_cb):
    def server():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("", UPLOAD_PORT))
        srv.listen(8)
        log_cb(f"[MAIN] Upload server listening on {UPLOAD_PORT}")
        while True:
            conn, addr = srv.accept()
            try:
                hlen_b = conn.recv(4)
                if not hlen_b:
                    conn.close()
                    continue
                hlen = struct.unpack("!I", hlen_b)[0]
                header = json.loads(conn.recv(hlen).decode("utf-8"))
                frame_num = int(header["frame"])
                filename = header["filename"]
                size = struct.unpack("!Q", conn.recv(8))[0]
                # Decide current render dir using callback (based on current .blend selection)
                render_dir = get_render_dir_fn()
                out_path = render_dir / filename
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, "wb") as f:
                    remaining = size
                    while remaining > 0:
                        chunk = conn.recv(min(4096, remaining))
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                log_cb(f"[MAIN] Received frame {frame_num} from {addr[0]} -> {out_path.name}")
                progress_cb(frame_num)
            except Exception as e:
                log_cb(f"[ERROR] Frame upload: {e}")
            finally:
                conn.close()
    threading.Thread(target=server, daemon=True).start()

# =========================
# Network discovery (UDP broadcast)
# =========================

# ip -> {"hostname":..., "port": int, "checkbox": ft.Checkbox, "selected": bool}
clients = {}

def set_client_selected(ip, val):
    if ip in clients:
        clients[ip]["selected"] = val

def discover_once(page, clients_box):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(1)
    try:
        sock.sendto(DISCOVERY_MAGIC, ("255.255.255.255", DISCOVERY_PORT))
        while True:
            data, addr = sock.recvfrom(1024)
            msg = data.decode().split("|")
            # expected: CLIENT|hostname|your_ip_seen_by_client|JOB_PORT
            if len(msg) >= 4 and msg[0] == "CLIENT":
                ip = addr[0]
                hostname = msg[1]
                try:
                    job_port = int(msg[3])
                except:
                    job_port = JOB_PORT_DEFAULT
                if ip not in clients:
                    cb = ft.Checkbox(
                        label=f"{hostname} ({ip}:{job_port})",
                        value=True,
                        on_change=lambda e, ip=ip: set_client_selected(ip, e.control.value)
                    )
                    clients[ip] = {"hostname": hostname, "port": job_port, "checkbox": cb, "selected": True}
                    clients_box.controls.append(cb)
                else:
                    clients[ip]["hostname"] = hostname
                    clients[ip]["port"] = job_port
                    clients[ip]["checkbox"].label = f"{hostname} ({ip}:{job_port})"
                page.update()
    except socket.timeout:
        pass
    finally:
        sock.close()

def discovery_loop(page, clients_box):
    while True:
        discover_once(page, clients_box)
        time.sleep(3)

# =========================
# Flet UI
# =========================

def main(page: ft.Page):
    page.title = "Blender Render (Flet Desktop)"
    page.scroll = "auto"
    page.window.height = 730

    # ---------- Load/save settings ----------
    def load_settings():
        if CONFIG_PATH.exists():
            try:
                return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
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
            CONFIG_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
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

    # File list + drop
    file_list = ft.ListView(expand=1, height=120, spacing=5)
    log_box = ft.ListView(expand=1, spacing=2, height=200, auto_scroll=True)
    elapsed_label = ft.Text("Elapsed: 00:00:00 | Remaining: --:--:--")

    # Grid
    frame_squares = []
    grid = ft.GridView(expand=1, runs_count=40, max_extent=20, spacing=1, run_spacing=1, height=200)

    # Worker state
    start_time = [None]
    stop_flag = threading.Event()
    local_worker = [None]
    scene_start = [None]
    scene_end = [None]

    # Time estimate (session-based)
    existing_set_current = set()
    missing_set_current = set()
    rendered_now = set()
    total_to_render = [0]

    # ---------------- Log buffer ----------------
    log_buffer = []
    MAX_LOG_LINES = 500

    def log_fn(msg):
        log_buffer.append(msg)

    def flush_logs():
        while True:
            if log_buffer:
                lines = list(log_buffer)
                log_buffer.clear()
                for line in lines:
                    log_box.controls.append(ft.Text(line))
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
        # mark square
        if scene_start[0] is None:
            return
        idx = f - scene_start[0]
        if 0 <= idx < len(frame_squares):
            frame_squares[idx].bgcolor = ft.Colors.GREEN
            frame_squares[idx].update()

        # count only frames we render in THIS session
        if f in missing_set_current:
            rendered_now.add(f)

        # time estimates based on session-only frames
        elapsed = time.time() - (start_time[0] or time.time())
        done_now = len(rendered_now)
        remain_frames = max(0, total_to_render[0] - done_now)

        remaining = (elapsed / done_now * remain_frames) if done_now > 0 else 0

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

    # ------------ Upload server uses current file's render dir -------------
    current_blend_file = [None]

    def get_render_dir():
        if not current_blend_file[0]:
            # default to out_root/Unknown
            return (Path(out_root.value).resolve() / "RENDERS").resolve()
        return (Path(out_root.value).resolve() / current_blend_file[0].stem).resolve()

    start_upload_server(get_render_dir, progress_cb, log_fn)

    # ---------------- DISTRIBUTED Start Render ----------------
    def start_render(e):
        # Use the FIRST .blend in the list for now (you can extend later)
        if not file_list.controls:
            log_fn("No files selected.")
            return

        blend_file = Path(file_list.controls[0].value).resolve()
        if not blend_file.exists():
            log_fn(f"[ERROR] File not found: {blend_file}")
            return
        current_blend_file[0] = blend_file

        # Frame range & existing
        try:
            s_start, s_end = get_blend_frame_range(blender_path.value, blend_file)
        except Exception as ex:
            log_fn(f"[ERROR] {ex}")
            return

        render_dir = Path(out_root.value).resolve() / blend_file.stem
        existing = get_existing_frames(render_dir)
        page.run_thread(lambda: grid_cb(s_start, s_end, existing))

        # Missing frames
        all_frames = list(range(s_start, s_end+1))
        missing = [f for f in all_frames if f not in existing]
        if not missing:
            log_fn("[INFO] No missing frames.")
            return

        # Build chunks (contiguous + chunk size)
        try:
            cs = max(1, int(chunk_size.value.strip() or "100"))
        except:
            cs = 100
        ranges = list(contiguous_ranges(sorted(missing)))
        chunks = list(split_ranges_by_chunk(ranges, cs))  # list of (a,b)

        # Choose workers: main + active clients; assign round-robin so main starts from earliest frames
        active_clients = [(ip, info) for ip, info in clients.items() if info.get("selected")]
        workers = [("local", None)] + [("client", (ip, info)) for ip, info in active_clients]

        # Partition chunks
        assignments = {("local", None): []}
        for ip, info in active_clients:
            assignments[("client", (ip, info))] = []
        for i, ch in enumerate(chunks):
            w = workers[i % len(workers)]
            assignments[w].append(ch)

        # Time estimate baseline
        start_time[0] = time.time()
        existing_set = set(existing)
        full = set(range(s_start, s_end + 1))
        missing_set = full - existing_set
        missing_set_current.clear()
        missing_set_current |= missing_set
        rendered_now.clear()
        total_to_render[0] = len(missing_set)

        # ---- LOCAL run (assigned chunks) ----
        stop_flag.clear()
        local_chunks = assignments[("local", None)]
        if local_worker[0] and local_worker[0].is_alive():
            log_fn("[WARN] Local worker already running.")
        else:
            if local_chunks:
                local_worker[0] = LocalChunksWorker(
                    blender_path.value,
                    blend_file,
                    render_dir,
                    local_chunks,
                    run_script.value,
                    script_name.value,
                    stop_flag,
                    log_fn,
                    lambda f: page.run_thread(lambda: progress_cb(f)),
                )
                local_worker[0].start()
                log_fn(f"[LOCAL] Assigned {len(local_chunks)} chunk(s).")
            else:
                log_fn("[LOCAL] No local chunks (all to clients).")

        # ---- DEPENDENCIES for clients ----
        dep_pairs = []
        try:
            dep_pairs = get_blend_dependencies(blender_path.value, blend_file)  # list of (abs, remote_rel)
        except Exception as ex:
            log_fn(f"[ERROR] Dependency scan: {ex}")
            dep_pairs = []

        dep_remote_list = [remote for (_a, remote) in dep_pairs]
        files_to_send = [(str(blend_file), blend_file.name)] + dep_pairs  # blend first

        # ---- Assign to clients ----
        for (kind, payload), cl_chunks in assignments.items():
            if kind != "client":
                continue
            (ip, info) = payload
            if not cl_chunks:
                continue
            # Merge this client's chunks into a single (start,end) to reduce launches
            a = cl_chunks[0][0]
            b = cl_chunks[-1][1]
            header = {
                "cmd": "render",
                "job_id": f"{blend_file.stem}_{a}-{b}".replace(" ", "_"),
                "file": blend_file.name,
                "dependencies": dep_remote_list,
                "start": a,
                "end": b,
                "upload_host": socket.gethostbyname(socket.gethostname()),
                "upload_port": UPLOAD_PORT,
                "run_script": bool(run_script.value),
                "script_name": script_name.value.strip(),
            }
            # Send job + files
            threading.Thread(
                target=send_job,
                args=(ip, info["port"], header, files_to_send, log_fn),
                daemon=True,
            ).start()
            log_fn(f"[MAIN] Assigned {len(cl_chunks)} chunk(s) to {ip}")

        log_fn("[MAIN] Distribution complete. Rendering in progress…")

    def stop_render(e):
        if local_worker[0] and local_worker[0].is_alive():
            stop_flag.set()
            log_fn("[LOCAL] Stop requested.")
        else:
            log_fn("Nothing running locally. (Client jobs will continue.)")

    # ---------------- Layout ----------------
    clients_box = ft.Column([], scroll="auto", width=250)
    threading.Thread(target=lambda: discovery_loop(page, clients_box), daemon=True).start()

    # Top row: controls + clients (top-right)
    top_row = ft.Row(
        [
            ft.Column([
                ft.Row([blender_path, ft.ElevatedButton("Browse…", on_click=lambda e: browse_exe(e))]),
                ft.Row([out_root, ft.ElevatedButton("Browse…", on_click=lambda e: browse_out(e))]),
                ft.Row([chunk_size, run_script, script_name]),
                ft.Row([
                    ft.ElevatedButton("Add Files…", on_click=add_files),
                    ft.ElevatedButton("Remove Selected", on_click=remove_selected),
                    ft.ElevatedButton("Clear List", on_click=clear_list),
                ]),
            ], alignment=ft.MainAxisAlignment.START),

            ft.Column([
                ft.Text("Clients:"),
                clients_box
            ], width=250, alignment=ft.MainAxisAlignment.START),
        ],
        alignment=ft.MainAxisAlignment.SPACE_BETWEEN
    )

    # Assemble page
    page.add(
        ft.Column(
            [
                top_row,
                ft.Text("Files:"),
                file_dropzone,
                ft.Text("Frames:"),
                grid,
                ft.Row(
                    [
                        elapsed_label,
                        ft.Row([
                            ft.ElevatedButton("Start", on_click=start_render),
                            ft.ElevatedButton("Stop", on_click=stop_render),
                        ])
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Text("Log:"),
                log_box,
            ],
            scroll="auto"
        )
    )

# Run
ft.app(target=main)
