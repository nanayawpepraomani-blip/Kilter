@echo off
setlocal EnableDelayedExpansion
title Kilter Setup

echo.
echo  ============================================================
echo   Kilter - First-Time Setup
echo  ============================================================
echo.

REM ----------------------------------------------------------------
REM  Check Python 3.12+
REM ----------------------------------------------------------------
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found.
    echo.
    echo  Install Python 3.12 or newer from:
    echo    https://www.python.org/downloads/
    echo.
    echo  When installing, tick "Add Python to PATH" before clicking Install.
    echo.
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo  Python found: %PYVER%

REM ----------------------------------------------------------------
REM  Create virtual environment
REM ----------------------------------------------------------------
if not exist ".venv" (
    echo  Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo  ERROR: Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo  Virtual environment created.
) else (
    echo  Virtual environment already exists, skipping.
)

REM ----------------------------------------------------------------
REM  Install dependencies
REM ----------------------------------------------------------------
echo  Installing dependencies (this may take a minute on first run)...
.venv\Scripts\pip install --quiet -r requirements.txt
if errorlevel 1 (
    echo  ERROR: pip install failed. Check your internet connection.
    pause
    exit /b 1
)
echo  Dependencies installed.

REM ----------------------------------------------------------------
REM  Generate .env if it doesn't exist
REM ----------------------------------------------------------------
if not exist ".env" (
    echo  Generating encryption key and creating .env...
    for /f "tokens=*" %%k in ('.venv\Scripts\python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"') do set GENKEY=%%k
    if "!GENKEY!"=="" (
        echo  ERROR: Could not generate encryption key.
        pause
        exit /b 1
    )
    copy .env.example .env >nul
    powershell -Command "(Get-Content .env) -replace 'KILTER_SECRET_KEY=$', 'KILTER_SECRET_KEY=!GENKEY!' | Set-Content .env"
    echo  .env created.
) else (
    echo  .env already exists, skipping key generation.
)

REM ----------------------------------------------------------------
REM  Ready — start the server
REM ----------------------------------------------------------------
echo.
echo  ============================================================
echo   Starting Kilter on http://localhost:8000
echo  ============================================================
echo.
echo  FIRST TIME? Watch the output below for a line that says:
echo.
echo    Open: http://localhost:8000/enroll?user=admin^&token=...
echo.
echo  Copy that URL into your browser to set up your login.
echo  A file called first_login.txt will also appear in this folder
echo  with the same link — open it if you miss it in the output.
echo.
echo  Press Ctrl+C to stop the server.
echo  ============================================================
echo.

.venv\Scripts\python -m uvicorn app:app --host 127.0.0.1 --port 8000

pause
