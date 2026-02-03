#!/usr/bin/env python3
"""
Test Flask routes directly
"""

from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def main_dashboard():
    return render_template('main_dashboard.html')

@app.route('/parquet')
def parquet_dashboard():
    return render_template('parquet_dashboard.html')

@app.route('/duckdb')
def duckdb_dashboard():
    return render_template('duckdb_dashboard.html')

@app.route('/etl')
def etl_dashboard():
    return render_template('etl_dashboard.html')

@app.route('/snowflake')
def snowflake_dashboard():
    return render_template('snowflake_dashboard.html')

@app.route('/test')
def test_route():
    return "Test route works!"

if __name__ == '__main__':
    print("Starting test Flask app...")
    print("Routes available:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule}")
    
    app.run(debug=True, host='0.0.0.0', port=5001)