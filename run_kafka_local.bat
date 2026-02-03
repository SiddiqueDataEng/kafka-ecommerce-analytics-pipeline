@echo off
echo Starting Kafka in KRaft mode (no Zookeeper needed)...

REM Create kafka directory if it doesn't exist
if not exist "kafka" mkdir kafka
cd kafka

REM Download Kafka if not already present
if not exist "kafka_2.13-3.6.0" (
    echo Downloading Kafka 3.6.0...
    curl -L -O https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
    tar -xzf kafka_2.13-3.6.0.tgz
    del kafka_2.13-3.6.0.tgz
)

cd kafka_2.13-3.6.0

REM Generate cluster UUID
echo Generating cluster UUID...
for /f %%i in ('bin\windows\kafka-storage.bat random-uuid') do set KAFKA_CLUSTER_ID=%%i

REM Format storage
echo Formatting storage with cluster ID: %KAFKA_CLUSTER_ID%
bin\windows\kafka-storage.bat format -t %KAFKA_CLUSTER_ID% -c config\kraft\server.properties

echo.
echo Starting Kafka server...
start "Kafka Server" bin\windows\kafka-server-start.bat config\kraft\server.properties

echo.
echo Waiting for Kafka to start...
timeout /t 10

echo Creating topics...
bin\windows\kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin\windows\kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

echo.
echo Kafka is running! Topics created.
echo You can now run your Python scripts.
echo.
echo To stop Kafka, close the Kafka Server window or press Ctrl+C
pause