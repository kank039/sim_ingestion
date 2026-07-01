@echo off
echo =========================================
echo       Starting Simulation Web UI
echo =========================================
echo.

:: Navigate to the parent directory (project root)
cd /d "%~dp0.."

echo Starting backend and frontend concurrently...
call npm run dev

pause
