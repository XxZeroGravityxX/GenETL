@echo off

: Set variables

:: Environment name
set env_name=gen_etl
:: Anaconda path
set anaconda_path="%USERPROFILE%\anaconda3\Scripts\activate.bat"
:: Python version
set py_version=3.11

: Build the package

:: Check if conda is available
if not exist "%anaconda_path%" (
    echo "Conda script not found at %anaconda_path%, skipping environment creation..."
) else (
    call "%anaconda_path%"
)
:: Check if environment exists
conda env list | findstr /I "%env_name%" >nul
if %ERRORLEVEL%==0 (
    echo "Conda environment %env_name% already exists, skipping creation..."
) else (
    conda create -n %env_name% python=%py_version% -y
)
:: Activate environment
call conda activate %env_name%
if %ERRORLEVEL%==0 (
    echo "Activated conda environment %env_name%"
) else (
    echo "Failed to activate conda environment %env_name%. Aborting..."
    exit /b 1
)
:: Install requirements
if exist requirements.txt (
    pip install -r requirements.txt
) else (
    echo "requirements.txt not found. Aborting..."
    exit /b 1
)
:: Make build
python build\build_pkg.py
