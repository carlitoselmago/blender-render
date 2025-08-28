# blender-render
A simple python script with GUI with drag and drop that processes blender files automatically for animations, checking the frame count and auto adjusting the missing frames to render

## Extra features
- It runs in a configurable batch size for renders that get slower over time, so you can configure a chunk size and blender will restart after each batch
- It can run scripts on starting the blender file

## Installation
Create a conda env
run:
```
conda install tk
pip install tkinterdnd2
```

There's a blender-render.bat file for windows, adjust to your conda env name, (default "base")


![alt text](https://github.com/carlitoselmago/blender-render/blob/main/assets/blender-render.jpg?raw=true)