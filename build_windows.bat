@echo off
REM ===== Simple one-file build for Windows with PyInstaller =====
REM Requirements:
REM   pip install pyinstaller
REM Optional:
REM   put an ICO at .\icon.ico (or change path below)

set SCRIPT=blender_render_gui.py
set ICON=icon.ico
set NAME=BlenderRenderGUI

if not exist venv (
  echo Creating venv...
  py -3 -m venv venv
)
call venv\Scripts\activate

echo Installing requirements...
pip install --upgrade pip
pip install pyinstaller tkinterdnd2

if exist "%ICON%" (
  set ICON_ARG=--icon "%ICON%"
) else (
  echo [WARN] icon.ico not found, building without custom icon.
  set ICON_ARG=
)

echo Building...
pyinstaller --noconfirm ^
  --onefile ^
  --windowed ^
  %ICON_ARG% ^
  --name "%NAME%" ^
  "%SCRIPT%"

echo.
echo ===== Build finished =====
echo Output: .\dist\%NAME%.exe
echo.
pause
