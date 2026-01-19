@echo off
REM Startup script for Axis replication system
echo Starting Axis Replication System...
echo.

cd /d "%~dp0.."
set PYTHONPATH=%CD%;%CD%\src

echo PYTHONPATH set to: %PYTHONPATH%
echo.

.\venv\Scripts\python.exe -m src.cli --replicas "postgresql://postgres:password@192.168.29.5:5433/replica_db"
