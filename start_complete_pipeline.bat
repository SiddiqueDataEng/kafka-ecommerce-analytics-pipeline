@echo off
echo ========================================
echo   Kafka Streaming Pipeline Launcher
echo ========================================
echo.

echo Step 1: Starting Kafka Server...
start "Kafka Server" cmd /k "run_kafka_local.bat"

echo Waiting for Kafka to initialize...
timeout /t 15

echo.
echo Step 2: Starting Web Dashboard...
start "Dashboard" cmd /k "run_ui.bat"

echo Waiting for dashboard to start...
timeout /t 5

echo.
echo Step 3: Starting Producer...
start "Producer" cmd /k "run_producer.bat"

echo Waiting for producer to start...
timeout /t 3

echo.
echo Step 4: Starting Stream Processor...
start "Stream Processor" cmd /k "run_stream_processor.bat"

echo.
echo ========================================
echo   🚀 PIPELINE STARTED SUCCESSFULLY!
echo ========================================
echo.
echo 📊 Dashboard: http://localhost:5000
echo 🔧 Kafka UI: http://localhost:8080 (if using Docker)
echo.
echo Windows opened:
echo - Kafka Server
echo - Web Dashboard  
echo - Event Producer
echo - Stream Processor
echo.
echo To start Snowflake consumer, run: run_snowflake_consumer.bat
echo (Make sure to update Snowflake credentials first)
echo.
pause