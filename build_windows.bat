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

REM --- Locate tkinterdnd2\tkdnd so we can bundle it ---
for /f "usebackq delims=" %%D in (`python -c "import os, tkinterdnd2; print(os.path.join(os.path.dirname(tkinterdnd2.__file__), 'tkdnd'))"`) do set TKDND_SRC=%%D

if not exist "%TKDND_SRC%" (
  echo [ERROR] Could not find tkdnd folder inside tkinterdnd2.
  echo         Looked at: %TKDND_SRC%
  echo         Make sure 'pip install tkinterdnd2' succeeded in this venv.
  exit /b 1
)

echo tkdnd folder: "%TKDND_SRC%"

REM On Windows, --add-data uses a semicolon:  src;dest_in_bundle
set TKDND_ADD=--add-data "%TKDND_SRC%;tkdnd"

if exist "%ICON%" (
  set ICON_ARG=--icon "%ICON%"
) else (
  echo [WARN] icon.ico not found, building without custom icon.
  set ICON_ARG=
)

echo Cleaning old build...
rmdir /s /q build 2>nul
rmdir /s /q dist 2>nul
del /q "%NAME%.spec" 2>nul

echo Building...
pyinstaller --noconfirm ^
  --onefile ^
  --windowed ^
  %ICON_ARG% ^
  %TKDND_ADD% ^
  --hidden-import tkinterdnd2 ^
  --name "%NAME%" ^
  "%SCRIPT%"

echo.
echo ===== Build finished =====
echo Output: .\dist\%NAME%.exe
echo.
pause
