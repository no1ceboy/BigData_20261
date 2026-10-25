@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
set "PYTHONPATH=%PROJECT_ROOT%\src"
if exist "%PROJECT_ROOT%\.venv\Scripts\python.exe" (
    "%PROJECT_ROOT%\.venv\Scripts\python.exe" -m freewaypulse.platform %*
) else (
    python -m freewaypulse.platform %*
)
exit /b %errorlevel%
