@echo off
echo Starting Kafka Pipeline Dashboard...
echo.
echo Dashboard will be available at: http://localhost:5000
echo.
call kafka_venv\Scripts\activate.bat
python app.py