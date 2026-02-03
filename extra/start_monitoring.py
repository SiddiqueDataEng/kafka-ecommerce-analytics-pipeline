#!/usr/bin/env python3
import requests
import json

def start_monitoring():
    try:
        print("Starting dashboard monitoring...")
        response = requests.post("http://localhost:5004/api/start_monitoring", timeout=5)
        if response.status_code == 200:
            print("✅ Monitoring started successfully!")
            return True
        else:
            print(f"❌ Failed to start monitoring: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error starting monitoring: {e}")
        return False

if __name__ == "__main__":
    start_monitoring()