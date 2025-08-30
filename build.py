# build.py
import os
from pathlib import Path
import sys

def find_tkdnd_data():
    """Return --add-data entries for tkinterdnd2 tkdnd folder if present."""
    adds = []
    try:
        import tkinterdnd2
        base = Path(tkinterdnd2.__file__).parent
        tkdnd = base / "tkdnd"
        if tkdnd.exists():
            # Windows add-data format is SRC;DST
            adds.append(f"{tkdnd}{os.pathsep}tkdnd")
    except Exception:
        pass
    return adds

def main():
    # ---- Adjust these if needed ----
    script = "blender_render_gui.py"   # put your actual filename here
    name   = "BlenderRenderGUI"
    icon   = "icon.ico"                # optional

    script_path = Path(script)
    if not script_path.exists():
        print(f"ERROR: Script file '{script}' not found in: {Path.cwd()}")
        sys.exit(1)

    try:
        from PyInstaller.__main__ import run as pyinstaller_run
    except Exception:
        print("PyInstaller not found. Install it with:\n  pip install pyinstaller")
        sys.exit(1)

    args = [
        "--noconfirm",
        "--onefile",
        "--windowed",
        f"--name={name}",
    ]
    if Path(icon).exists():
        args.append(f"--icon={icon}")
    else:
        print("[WARN] icon.ico not found; continuing without custom icon.")

    # Bundle tkdnd (drag & drop for tkinterdnd2)
    for add in find_tkdnd_data():
        args += ["--add-data", add]

    args.append(str(script_path))

    print("Running PyInstaller with:", " ".join(args))
    pyinstaller_run(args)

    print("\n===== Build finished =====")
    print(f"Output: .\\dist\\{name}.exe")

if __name__ == "__main__":
    main()
