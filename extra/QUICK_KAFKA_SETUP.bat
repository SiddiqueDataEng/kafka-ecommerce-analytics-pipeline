@echo off
cls
echo.
echo ========================================
echo   QUICK KAFKA SETUP
echo ========================================
echo.
echo This will download and start Kafka locally
echo.

cd kafka
echo Downloading Kafka 2.13-3.6.0...
echo This may take a few minutes...

powershell -Command "Invoke-WebRequest -Uri 'https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz' -OutFile 'kafka.tgz'"

if not exist kafka.tgz (
    echo Failed to download Kafka. Trying alternative URL...
    powershell -Command "Invoke-WebRequest -Uri 'https://archive.apache.org/dist/kafka/3.6.0/kafka_2.13-3.6.0.tgz' -OutFile 'kafka.tgz'"
)

if not exist kafka.tgz (
    echo Failed to download Kafka. Please check your internet connection.
    pause
    exit /b 1
)

echo.
echo Extracting Kafka...
powershell -Command "tar -xzf kafka.tgz"

if exist kafka_2.13-3.6.0 (
    echo.
    echo ✅ Kafka downloaded and extracted successfully!
    echo.
    cd kafka_2.13-3.6.0
    
    echo Starting Zookeeper...
    start "Zookeeper" cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"
    
    echo Waiting for Zookeeper to start...
    timeout /t 15
    
    echo Starting Kafka Server...
    start "Kafka Server" cmd /k "bin\windows\kafka-server-start.bat config\server.properties"
    
    echo Waiting for Kafka to start...
    timeout /t 20
    
    echo.
    echo Creating topics...
    bin\windows\kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    bin\windows\kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    
    echo.
    echo ✅ Kafka is now running!
    echo ✅ Topics created: raw_events, clean_events
    echo.
    echo Kafka Server: localhost:9092
    echo Zookeeper: localhost:2181
    echo.
    echo You can now run your dashboard!
    echo.
) else (
    echo Failed to extract Kafka.
    pause
    exit /b 1
)

cd ..
cd ..
echo.
echo ========================================
echo   KAFKA READY!
echo ========================================
echo.
echo Now run: run_multi_dashboard.bat
echo Then open: http://localhost:5004/kafka
echo.
pause