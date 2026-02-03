#!/usr/bin/env python3
import requests
import json

def test_dashboard_events():
    try:
        print("Testing dashboard events...")
        response = requests.get("http://localhost:5004/api/recent_events/raw", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Recent raw events count: {len(data)}")
            if data:
                print(f"✅ Latest event: {json.dumps(data[-1], indent=2)}")
            return True
        else:
            print(f"❌ Dashboard API error: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Dashboard connection error: {e}")
        return False

if __name__ == "__main__":
    test_dashboard_events()