#!/usr/bin/env python3
"""
Debug version of multi_dashboard_app.py to identify the issue
"""

from flask import Flask, render_template, jsonify, request
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-dashboard-secret'

print("Flask app created successfully")

# Try to import components one by one
try:
    from data_generators import EcommerceDataGenerator
    print("✅ EcommerceDataGenerator imported successfully")
    data_generator = EcommerceDataGenerator()
    print("✅ EcommerceDataGenerator initialized successfully")
except Exception as e:
    print(f"❌ Error with EcommerceDataGenerator: {e}")
    data_generator = None

try:
    from multi_storage_manager import MultiStorageManager
    print("✅ MultiStorageManager imported successfully")
    storage_manager = MultiStorageManager()
    print("✅ MultiStorageManager initialized successfully")
except Exception as e:
    print(f"❌ Error with MultiStorageManager: {e}")
    storage_manager = None

try:
    from etl_pipeline import ETLPipeline
    print("✅ ETLPipeline imported successfully")
    etl_pipeline = ETLPipeline()
    print("✅ ETLPipeline initialized successfully")
except Exception as e:
    print(f"❌ Error with ETLPipeline: {e}")
    etl_pipeline = None

# Define routes
@app.route('/')
def main_dashboard():
    print("Main dashboard route called")
    return render_template('main_dashboard.html')

@app.route('/parquet')
def parquet_dashboard():
    print("Parquet dashboard route called")
    return render_template('parquet_dashboard.html')

@app.route('/duckdb')
def duckdb_dashboard():
    print("DuckDB dashboard route called")
    return render_template('duckdb_dashboard.html')

@app.route('/etl')
def etl_dashboard():
    print("ETL dashboard route called")
    return render_template('etl_dashboard.html')

@app.route('/snowflake')
def snowflake_dashboard():
    print("Snowflake dashboard route called")
    return render_template('snowflake_dashboard.html')

@app.route('/api/stats')
def get_stats():
    print("API stats route called")
    return jsonify({'status': 'debug mode', 'components_loaded': {
        'data_generator': data_generator is not None,
        'storage_manager': storage_manager is not None,
        'etl_pipeline': etl_pipeline is not None
    }})

print("Routes defined successfully")

if __name__ == '__main__':
    print("Starting debug Flask app...")
    print("Routes available:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule}")
    
    app.run(debug=True, host='0.0.0.0', port=5002)