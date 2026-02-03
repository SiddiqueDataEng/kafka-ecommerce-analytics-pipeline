@echo off
echo ========================================
echo   Kafka Pipeline Demo Dashboard
echo ========================================
echo.
echo This demo simulates the Kafka streaming pipeline
echo without requiring actual Kafka installation.
echo.
echo Dashboard will be available at: http://localhost:5000
echo.
echo Features:
echo - Real-time event generation
echo - Stream processing simulation  
echo - Interactive charts and statistics
echo - Event monitoring and filtering
echo.
call kafka_venv\Scripts\activate.bat
python app_demo.py