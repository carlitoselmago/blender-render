@echo off
REM ===== One-file build for Windows with PyInstaller (tkdnd robust) =====

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

REM ---- Find a tkdnd folder that actually has pkgIndex.tcl ----
for /f "usebackq delims=" %%D in (`
  python -c "import os,site,sys,glob; 
try:
    import tkinterdnd2
    cand=[os.path.join(os.path.dirname(tkinterdnd2.__file__),'tkdnd')]
except Exception:
    cand=[];
paths=set(site.getsitepackages()+[site.getusersitepackages()]);
cp=os.environ.get('CONDA_PREFIX');
if cp: paths.add(os.path.join(cp,'Lib','site-packages'));
# also try the common Anaconda system location
paths.add(r'C:\ProgramData\anaconda3\Lib\site-packages');
cands=cand+[os.path.join(p,'tkinterdnd2','tkdnd') for p in paths];
for p in cands:
    if os.path.isdir(p) and os.path.isfile(os.path.join(p,'pkgIndex.tcl')):
        print(p); sys.exit(0)
print(''); sys.exit(1)"
`) do set "TKDND_SRC=%%D"

if "%TKDND_SRC%"=="" (
  echo [ERROR] Could not locate a valid tkdnd folder with pkgIndex.tcl.
  echo         Ensure tkinterdnd2 is installed somewhere that includes the native tkdnd.
  exit /b 1
)

echo Using tkdnd from: "%TKDND_SRC%"

REM Windows uses semicolon in --add-data
set "TKDND_ADD=--add-data=%TKDND_SRC%;tkdnd"

if exist "%ICON%" (
  set "ICON_ARG=--icon=%ICON%"
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
  --collect-all tkinterdnd2 ^
  --name "%NAME%" ^
  "%SCRIPT%"

echo.
echo ===== Build finished =====
echo Output: .\dist\%NAME%.exe
echo.
pause
