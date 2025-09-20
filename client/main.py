import flet as ft
import threading
import socket
import json
import struct
import subprocess
import os
import re
from pathlib import Path

CONFIG_PATH = Path(__file__).with_name(".blender_render_client.json")

DISCOVERY_PORT = 50000
DISCOVERY_MAGIC = b"BLENDER_DISCOVER"
JOB_PORT = 50010  # TCP job server

# =========================
# Helpers
# =========================

def recv_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def send_frame(upload_host, upload_port, img_path: Path, log_fn):
    try:
        log_fn(f"[CLIENT] Preparing to upload {img_path} ({img_path.stat().st_size} bytes) "
               f"to {upload_host}:{upload_port}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((upload_host, upload_port))
        log_fn(f"[CLIENT] Connected to {upload_host}:{upload_port}")

        # frame number from filename
        m = re.search(r'(\d+)$', img_path.stem)
        frame_num = int(m.group(1)) if m else -1
        header = {"frame": frame_num, "filename": img_path.name}
        hdr = json.dumps(header).encode("utf-8")
        sock.sendall(struct.pack("!I", len(hdr)))
        sock.sendall(hdr)

        size = img_path.stat().st_size
        sock.sendall(struct.pack("!Q", size))

        with open(img_path, "rb") as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                sock.sendall(chunk)

        sock.close()
        log_fn(f"[CLIENT] Uploaded {img_path.name} to {upload_host}:{upload_port}")
    except Exception as e:
        log_fn(f"[CLIENT] Upload error to {upload_host}:{upload_port}: {e}")


# =========================
# Job server (TCP)
# =========================

def start_job_server(get_blender_exe, log_fn):
    def server():
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", JOB_PORT))
        sock.listen(5)
        log_fn(f"[CLIENT] Job server listening on {JOB_PORT}")

        while True:
            conn, addr = sock.accept()
            try:
                hlen_b = recv_exact(conn, 4)
                if not hlen_b:
                    conn.close()
                    continue
                hlen = struct.unpack("!I", hlen_b)[0]
                header = json.loads(recv_exact(conn, hlen).decode("utf-8"))

                if header.get("cmd") != "render":
                    log_fn(f"[CLIENT] Unknown cmd: {header.get('cmd')}")
                    conn.close()
                    continue

                job_id = header.get("job_id", "job")
                blend_name = header["file"]
                deps = header.get("dependencies", [])
                start_f = int(header.get("start", 1))
                end_f = int(header.get("end", start_f))
                upload_host = header["upload_host"]
                upload_port = int(header["upload_port"])
                run_script = bool(header.get("run_script", False))
                script_name = header.get("script_name", "").strip()

                job_dir = Path("jobs") / job_id
                job_dir.mkdir(parents=True, exist_ok=True)

                # Receive .blend
                size = struct.unpack("!Q", recv_exact(conn, 8))[0]
                blend_path = job_dir / blend_name
                with open(blend_path, "wb") as f:
                    remaining = size
                    while remaining > 0:
                        chunk = conn.recv(min(4096, remaining))
                        if not chunk:
                            break
                        f.write(chunk)
                        remaining -= len(chunk)
                log_fn(f"[CLIENT] Received {blend_name} ({size} bytes)")

                # Receive dependencies in order declared
                for dep_rel in deps:
                    dep_path = job_dir / dep_rel
                    dep_path.parent.mkdir(parents=True, exist_ok=True)
                    size = struct.unpack("!Q", recv_exact(conn, 8))[0]
                    with open(dep_path, "wb") as f:
                        remaining = size
                        while remaining > 0:
                            chunk = conn.recv(min(4096, remaining))
                            if not chunk:
                                break
                            f.write(chunk)
                            remaining -= len(chunk)
                    log_fn(f"[CLIENT] Received dep {dep_rel}")

                conn.close()

                # Render
                frames_dir = job_dir / "frames"
                frames_dir.mkdir(parents=True, exist_ok=True)

                blender_exe = get_blender_exe()
                args = [str(blender_exe), "-b", str(blend_path)]
                if run_script and script_name:
                    args += ["--enable-autoexec", "--python-text", script_name]
                # (Optional) quick remap pass could be added here if needed
                args += [
                    "-s", str(start_f),
                    "-e", str(end_f),
                    "-o", str(frames_dir / "####"),
                    "-x", "1",
                    "-a",
                ]
                log_fn(f"[CLIENT] Running: {' '.join(args)}")
                proc = subprocess.Popen(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1
                )
                saved_re = re.compile(r"Saved:\s+'.*?[\\/](\d+)\.\w+'", re.IGNORECASE)
                saved_paths = []
                while True:
                    line = proc.stdout.readline()
                    if not line and proc.poll() is not None:
                        break
                    if not line:
                        continue
                    line = line.rstrip()
                    log_fn(line)
                    m = saved_re.search(line)
                    if m:
                        try:
                            saved_paths = sorted(frames_dir.iterdir())
                            if saved_paths:
                                last_img = saved_paths[-1]
                                log_fn(f"[CLIENT] Detected saved frame {last_img}, attempting upload...")
                                send_frame(upload_host, upload_port, last_img, log_fn)
                        except Exception as _e:
                            log_fn(f"[CLIENT] Exception during auto-upload: {_e}")
                code = proc.wait()
                log_fn(f"[CLIENT] Blender exited with {code}")

                # Upload any files not uploaded (safety)
                for img in sorted(frames_dir.iterdir()):
                    log_fn(f"[CLIENT] Safety re-upload for {img}")
                    send_frame(upload_host, upload_port, img, log_fn)

            except Exception as e:
                log_fn(f"[CLIENT] Job error: {e}")
            finally:
                try:
                    conn.close()
                except:
                    pass

    threading.Thread(target=server, daemon=True).start()

# =========================
# Discovery (UDP)
# =========================

def start_client_discovery():
    def listen():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        hostname = socket.gethostname()
        while True:
            data, addr = sock.recvfrom(1024)
            if data == DISCOVERY_MAGIC:
                # reply with job port so server can connect back
                reply = f"CLIENT|{hostname}|{addr[0]}|{JOB_PORT}".encode()
                sock.sendto(reply, addr)
    threading.Thread(target=listen, daemon=True).start()

# =========================
# Settings
# =========================

def load_settings():
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_settings_val(key, val):
    s = load_settings()
    s[key] = val
    CONFIG_PATH.write_text(json.dumps(s, indent=2, ensure_ascii=False), encoding="utf-8")

# =========================
# Flet UI
# =========================

def main(page: ft.Page):
    page.title = "Blender Render Client"
    page.window.height = 260
    page.scroll = "auto"

    settings = load_settings()

    blender_path = ft.TextField(
        label="Blender executable",
        value=settings.get("blender_exe", "blender"),
        width=500,
        on_change=lambda e: save_settings_val("blender_exe", blender_path.value.strip()),
    )

    log_view = ft.ListView(expand=1, spacing=2, height=150, auto_scroll=True)
    MAX_LOG = 400

    def log_fn(msg):
        log_view.controls.append(ft.Text(msg))
        if len(log_view.controls) > MAX_LOG:
            del log_view.controls[0:len(log_view.controls)-MAX_LOG]
        page.update()

    page.add(ft.Column([blender_path, ft.Text("Log:"), log_view]))

    # Start discovery + job server
    start_client_discovery()
    start_job_server(lambda: blender_path.value, log_fn)

ft.app(target=main)
