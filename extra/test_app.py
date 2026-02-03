#!/usr/bin/env python3
"""
Simple test to verify Flask app is working
"""

import requests
import time

def test_app():
    """Test if the Flask app is responding"""
    try:
        print("Testing Flask application routes...")
        print("=" * 50)
        
        # Test all dashboard routes
        routes = [
            ('Main Dashboard', 'http://localhost:5004'),
            ('Parquet Dashboard', 'http://localhost:5004/parquet'),
            ('DuckDB Dashboard', 'http://localhost:5004/duckdb'),
            ('ETL Dashboard', 'http://localhost:5004/etl'),
            ('Snowflake Dashboard', 'http://localhost:5004/snowflake'),
            ('API Stats', 'http://localhost:5004/api/stats'),
            ('API Storage Stats', 'http://localhost:5004/api/storage_stats')
        ]
        
        for name, url in routes:
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"✅ {name}: {response.status_code} - OK ({len(response.text)} chars)")
                else:
                    print(f"❌ {name}: {response.status_code} - ERROR")
                    if response.status_code == 404:
                        print(f"   Route not found: {url}")
                    elif response.status_code == 500:
                        print(f"   Server error - check logs")
            except Exception as e:
                print(f"❌ {name}: ERROR - {e}")
        
        print("=" * 50)
            
    except Exception as e:
        print(f"❌ General Error: {e}")

if __name__ == "__main__":
    test_app()