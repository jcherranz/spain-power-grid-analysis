@echo off
echo ========================================
echo Spain Power Grid Analysis Runner
echo ========================================
echo.

cd /d "%~dp0"

echo Installing/updating required packages...
pip install -r requirements.txt
echo.

echo Running connection test...
python code\test_connection.py
echo.

echo Press any key to run the analysis...
pause >nul

echo.
echo Running power grid analysis...
python code\simple_power_analyzer.py

echo.
echo ========================================
echo Analysis complete! Check outputs folder.
echo ========================================
pause