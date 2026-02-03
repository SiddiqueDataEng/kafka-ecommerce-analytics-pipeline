from flask import Flask, render_template, jsonify, request
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-dashboard-secret'

# Simple stats for testing
event_stats = {
    'raw_events_count': 0,
    'clean_events_count': 0,
    'invalid_events_count': 0,
    'page_views': 0,
    'product_interactions': 0,
    'purchases': 0,
    'searches': 0,
    'user_engagement': 0,
    'sessions_created': 0,
    'parquet_events': 0,
    'duckdb_events': 0,
    'data_quality_score': 100.0,
    'etl_runs': 0,
    'snowflake_records': 0
}

# Routes for different dashboards
@app.route('/')
def main_dashboard():
    logger.info("Main dashboard accessed")
    return render_template('main_dashboard.html')

@app.route('/parquet')
def parquet_dashboard():
    logger.info("Parquet dashboard accessed")
    return render_template('parquet_dashboard.html')

@app.route('/duckdb')
def duckdb_dashboard():
    logger.info("DuckDB dashboard accessed")
    return render_template('duckdb_dashboard.html')

@app.route('/etl')
def etl_dashboard():
    logger.info("ETL dashboard accessed")
    return render_template('etl_dashboard.html')

@app.route('/snowflake')
def snowflake_dashboard():
    logger.info("Snowflake dashboard accessed")
    return render_template('snowflake_dashboard.html')

# API Routes
@app.route('/api/stats')
def get_stats():
    logger.info("API stats accessed")
    return jsonify(event_stats)

@app.route('/api/storage_stats')
def get_storage_stats():
    logger.info("API storage stats accessed")
    return jsonify({'parquet': {'file_count': 0}, 'duckdb': {'event_count': 0}})

if __name__ == '__main__':
    logger.info("Starting Simple Multi-Dashboard...")
    logger.info("Routes registered:")
    for rule in app.url_map.iter_rules():
        logger.info(f"  {rule}")
    
    app.run(debug=True, host='0.0.0.0', port=5003)