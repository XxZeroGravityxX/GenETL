@echo off

REM Set variables
set env_name=gen_etl
set anaconda_path="%USERPROFILE%\anaconda3\Scripts\activate.bat"
set py_version=3.11

REM Check if conda is available
if not exist "%anaconda_path%" (
    echo "Conda script not found at %anaconda_path%, skipping environment creation..."
) else (
    call "%anaconda_path%"
)

REM Check if environment exists
conda env list | findstr /I "%env_name%" >nul
if %ERRORLEVEL%==0 (
    echo "Conda environment %env_name% already exists, skipping creation..."
) else (
    conda create -n %env_name% python=%py_version% -y
)

REM Activate environment
call conda activate %env_name%
if %ERRORLEVEL%==0 (
    echo "Activated conda environment %env_name%"
) else (
    echo "Failed to activate conda environment %env_name%. Aborting..."
    exit /b 1
)

REM Install requirements
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo "requirements.txt not found. Aborting..."
    exit /b 1
)

REM Change directory
cd /d "%~dp0\..\.."
echo "Building from %CD%"

REM Make build
python build\build_pkg.py
