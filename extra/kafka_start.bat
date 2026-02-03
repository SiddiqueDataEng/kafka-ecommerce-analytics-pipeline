@echo off
echo Starting Zookeeper...
cd /d "%~dp0kafka\kafka_2.13-3.6.0"
start "Zookeeper" cmd /k "bin\windows\zookeeper-server-start.bat config\zookeeper.properties"

echo Waiting 15 seconds for Zookeeper...
timeout /t 15 /nobreak

echo Starting Kafka Server...
start "Kafka Server" cmd /k "bin\windows\kafka-server-start.bat config\server.properties"

echo Waiting 20 seconds for Kafka...
timeout /t 20 /nobreak

echo Creating topics...
bin\windows\kafka-topics.bat --create --topic raw_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
bin\windows\kafka-topics.bat --create --topic clean_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo.
echo ✅ Kafka setup complete!
echo Kafka Server: localhost:9092
echo.
pause