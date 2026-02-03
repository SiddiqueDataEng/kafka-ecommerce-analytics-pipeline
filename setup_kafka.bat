@echo off
echo Setting up Kafka locally...

REM Create kafka directory if it doesn't exist
if not exist "kafka" mkdir kafka
cd kafka

REM Download Kafka if not already present
if not exist "kafka_2.13-2.8.2" (
    echo Downloading Kafka...
    curl -O https://downloads.apache.org/kafka/2.8.2/kafka_2.13-2.8.2.tgz
    tar -xzf kafka_2.13-2.8.2.tgz
    del kafka_2.13-2.8.2.tgz
)

cd kafka_2.13-2.8.2

echo Kafka setup complete!
echo.
echo To start Kafka:
echo 1. Start Zookeeper: bin\windows\zookeeper-server-start.bat config\zookeeper.properties
echo 2. Start Kafka: bin\windows\kafka-server-start.bat config\server.properties
echo.
echo To create topics:
echo bin\windows\kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
echo bin\windows\kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

pause