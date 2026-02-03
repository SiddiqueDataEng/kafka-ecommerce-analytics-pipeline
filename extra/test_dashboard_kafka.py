#!/usr/bin/env python3
import requests
import json

def test_dashboard_kafka():
    try:
        print("Testing dashboard Kafka integration...")
        response = requests.get("http://localhost:5004/api/kafka_stats", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Dashboard API response: {json.dumps(data, indent=2)}")
            return True
        else:
            print(f"❌ Dashboard API error: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Dashboard connection error: {e}")
        return False

if __name__ == "__main__":
    test_dashboard_kafka()