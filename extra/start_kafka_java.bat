@echo off
echo Starting Kafka with Java directly...

set KAFKA_HOME=%CD%\k
set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M
set KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:%KAFKA_HOME%\config\log4j.properties

echo Starting Zookeeper...
start "Zookeeper" java -cp "%KAFKA_HOME%\libs\*" -Dlog4j.configuration=file:%KAFKA_HOME%\config\log4j.properties org.apache.zookeeper.server.quorum.QuorumPeerMain %KAFKA_HOME%\config\zookeeper.properties

echo Waiting 15 seconds for Zookeeper...
timeout /t 15 /nobreak

echo Starting Kafka Server...
start "Kafka Server" java -cp "%KAFKA_HOME%\libs\*" -Dlog4j.configuration=file:%KAFKA_HOME%\config\log4j.properties -Dkafka.logs.dir=%KAFKA_HOME%\logs org.apache.kafka.Kafka %KAFKA_HOME%\config\server.properties

echo Waiting 20 seconds for Kafka...
timeout /t 20 /nobreak

echo Creating topics...
java -cp "%KAFKA_HOME%\libs\*" kafka.admin.TopicCommand --create --topic raw_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
java -cp "%KAFKA_HOME%\libs\*" kafka.admin.TopicCommand --create --topic clean_events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo.
echo ✅ Kafka setup complete!
echo Kafka Server: localhost:9092
echo.
pause