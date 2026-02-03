#!/usr/bin/env python3
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

def test_kafka_connection():
    try:
        print("Testing Kafka connection...")
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        print("✅ Kafka Producer connected successfully!")
        producer.close()
        return True
    except NoBrokersAvailable:
        print("❌ Kafka not available - no brokers found")
        return False
    except Exception as e:
        print(f"❌ Kafka connection error: {e}")
        return False

if __name__ == "__main__":
    if test_kafka_connection():
        print("🎉 Kafka is ready!")
        sys.exit(0)
    else:
        print("⚠️ Kafka is not ready yet")
        sys.exit(1)