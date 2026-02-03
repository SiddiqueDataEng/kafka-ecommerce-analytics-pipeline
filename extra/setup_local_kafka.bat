@echo off
cls
echo.
echo ========================================
echo   Local Kafka Setup
echo ========================================
echo.
echo This script will help you set up Kafka locally for the dashboard.
echo.
echo Option 1: Download and setup Kafka locally (Recommended)
echo Option 2: Use existing Kafka installation
echo Option 3: Use Docker Kafka (if Docker is available)
echo.
echo Choose your option:
echo [1] Download Kafka locally
echo [2] Use existing Kafka
echo [3] Use Docker
echo [4] Exit
echo.
set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" goto download_kafka
if "%choice%"=="2" goto existing_kafka
if "%choice%"=="3" goto docker_kafka
if "%choice%"=="4" goto exit
goto invalid_choice

:download_kafka
echo.
echo ========================================
echo   Downloading Kafka
echo ========================================
echo.
echo Downloading Kafka 2.13-3.6.0...
echo This may take a few minutes...
echo.

mkdir kafka_local 2>nul
cd kafka_local

echo Downloading from Apache Kafka...
powershell -Command "Invoke-WebRequest -Uri 'https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz' -OutFile 'kafka.tgz'"

if not exist kafka.tgz (
    echo Failed to download Kafka. Please check your internet connection.
    pause
    goto exit
)

echo.
echo Extracting Kafka...
powershell -Command "tar -xzf kafka.tgz"

if exist kafka_2.13-3.6.0 (
    echo.
    echo ✅ Kafka downloaded and extracted successfully!
    echo.
    echo Starting Kafka services...
    cd kafka_2.13-3.6.0
    
    echo Starting Zookeeper...
    start "Zookeeper" cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
    
    echo Waiting for Zookeeper to start...
    timeout /t 10
    
    echo Starting Kafka Server...
    start "Kafka Server" cmd /k "bin\windows\kafka-server-start.bat config\server.properties"
    
    echo Waiting for Kafka to start...
    timeout /t 15
    
    echo.
    echo Creating topics...
    bin\windows\kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    bin\windows\kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    
    echo.
    echo ✅ Kafka is now running!
    echo ✅ Topics created: raw_events, clean_events
    echo.
    echo Kafka Server: localhost:9092
    echo Zookeeper: localhost:2181
    echo.
    echo You can now use the Kafka features in your dashboard!
    echo.
) else (
    echo Failed to extract Kafka.
)

cd ..
cd ..
goto end

:existing_kafka
echo.
echo ========================================
echo   Using Existing Kafka
echo ========================================
echo.
echo Please make sure your Kafka is running on localhost:9092
echo.
echo If you need to start Kafka manually:
echo 1. Start Zookeeper first
echo 2. Start Kafka server
echo 3. Create topics: raw_events and clean_events
echo.
echo Example commands:
echo zookeeper-server-start.bat config\zookeeper.properties
echo kafka-server-start.bat config\server.properties
echo.
echo kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092
echo kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092
echo.
goto end

:docker_kafka
echo.
echo ========================================
echo   Docker Kafka Setup
echo ========================================
echo.
echo Creating docker-compose.yml for Kafka...
echo.

(
echo version: '3.8'
echo services:
echo   zookeeper:
echo     image: confluentinc/cp-zookeeper:latest
echo     environment:
echo       ZOOKEEPER_CLIENT_PORT: 2181
echo       ZOOKEEPER_TICK_TIME: 2000
echo     ports:
echo       - "2181:2181"
echo.
echo   kafka:
echo     image: confluentinc/cp-kafka:latest
echo     depends_on:
echo       - zookeeper
echo     ports:
echo       - "9092:9092"
echo     environment:
echo       KAFKA_BROKER_ID: 1
echo       KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
echo       KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
echo       KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
echo       KAFKA_AUTO_CREATE_TOPICS_ENABLE: true
) > docker-compose-kafka.yml

echo Docker Compose file created: docker-compose-kafka.yml
echo.
echo To start Kafka with Docker:
echo docker-compose -f docker-compose-kafka.yml up -d
echo.
echo To stop:
echo docker-compose -f docker-compose-kafka.yml down
echo.
goto end

:invalid_choice
echo Invalid choice. Please enter 1, 2, 3, or 4.
pause
goto setup_local_kafka

:end
echo.
echo ========================================
echo   Setup Complete
echo ========================================
echo.
echo Once Kafka is running, you can:
echo 1. Open your dashboard: http://localhost:5004/kafka
echo 2. Click "Start Kafka Producer" to generate events
echo 3. Run the stream processor: run_stream_processor.bat
echo.
echo Kafka should be available at: localhost:9092
echo.
pause
goto exit

:exit
echo.
echo Exiting setup...