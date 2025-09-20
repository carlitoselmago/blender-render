# blender-render
A simple python script with GUI with drag and drop that processes blender files automatically for animations, checking the frame count and auto adjusting the missing frames to render

## Extra features
- It runs in a configurable batch size for renders that get slower over time, so you can configure a chunk size and blender will restart after each batch
- It can run scripts on starting the blender file

## Installation
run:
```
python -m venv .venv
.venv\Scripts\activate.bat #(source .venv/bin/activate)
pip install -r requirements.txt

python main.py
```

The drop zone plugin requires that you build the app for desktop once first

## Build
run:
```
flet build windows -v
```

Note: if you encounter problems building because android incompatibilities, modify the file build/flutter/pubspec.yaml to this:
```
webview_flutter_android: ^3.8.0
```

There's a blender-render.bat file for windows, adjust to your conda env name, (default "base")

## Notes

The software will expect that the render output folder to be called exactly the same as the source blender file, then it will check how many frames are missing and restart from there
You still need to configure manually each blender file output settings the way you want

![alt text](https://github.com/carlitoselmago/blender-render/blob/main/assets/blender-render.jpg?raw=true)