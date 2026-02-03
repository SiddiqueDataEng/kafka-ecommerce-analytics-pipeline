@echo off
cls
echo.
echo ========================================
echo   Starting Kafka with Docker
echo ========================================
echo.
echo 🐳 Starting Kafka and Zookeeper containers...
echo.
echo This will start:
echo • Zookeeper on port 2181
echo • Kafka on port 9092
echo • Kafka UI on port 8080
echo.

docker-compose -f docker-compose-kafka.yml up -d

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ✅ Kafka containers started successfully!
    echo.
    echo 📊 Services available:
    echo • Kafka Server: localhost:9092
    echo • Kafka UI: http://localhost:8080
    echo • Zookeeper: localhost:2181
    echo.
    echo Waiting for Kafka to be ready...
    timeout /t 30
    echo.
    echo 🎯 Creating topics...
    docker exec kafka kafka-topics --create --topic raw_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    docker exec kafka kafka-topics --create --topic clean_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    echo.
    echo ✅ Topics created successfully!
    echo.
    echo 🚀 Kafka is ready! You can now:
    echo 1. Open your dashboard: http://localhost:5004/kafka
    echo 2. Click "Start Kafka Producer" to generate events
    echo 3. View Kafka UI: http://localhost:8080
    echo.
) else (
    echo.
    echo ❌ Failed to start Kafka containers.
    echo Please make sure Docker is installed and running.
    echo.
)

echo.
echo Press any key to continue...
pause >nul