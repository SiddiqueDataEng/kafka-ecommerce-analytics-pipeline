from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-dashboard-secret'

# Initialize components with error handling
data_generator = None
storage_manager = None
etl_pipeline = None

try:
    from data_generators import EcommerceDataGenerator
    data_generator = EcommerceDataGenerator()
    logger.info("EcommerceDataGenerator initialized successfully")
except Exception as e:
    logger.error(f"Error initializing EcommerceDataGenerator: {e}")

try:
    from multi_storage_manager import MultiStorageManager
    storage_manager = MultiStorageManager()
    logger.info("MultiStorageManager initialized successfully")
except Exception as e:
    logger.error(f"Error initializing MultiStorageManager: {e}")

try:
    from etl_pipeline import ETLPipeline
    etl_pipeline = ETLPipeline()
    logger.info("ETLPipeline initialized successfully")
except Exception as e:
    logger.error(f"Error initializing ETLPipeline: {e}")

# Global variables for monitoring
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
    'last_raw_event': None,
    'last_clean_event': None,
    'data_quality_score': 100.0,
    'etl_runs': 0,
    'snowflake_records': 0
}

recent_events = {
    'raw': [],
    'clean': [],
    'invalid': [],
    'parquet': [],
    'duckdb': []
}

# Thread control
threads = {}
stop_flags = {}

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
    return jsonify(event_stats)

@app.route('/api/storage_stats')
def get_storage_stats():
    if storage_manager:
        return jsonify(storage_manager.get_combined_stats())
    return jsonify({'error': 'Storage manager not available'})

@app.route('/api/parquet_stats')
def get_parquet_stats():
    if storage_manager:
        return jsonify(storage_manager.get_parquet_stats())
    return jsonify({'error': 'Storage manager not available'})

@app.route('/api/duckdb_stats')
def get_duckdb_stats():
    if storage_manager:
        return jsonify(storage_manager.get_duckdb_stats())
    return jsonify({'error': 'Storage manager not available'})

@app.route('/api/parquet_events')
def get_parquet_events():
    limit = request.args.get('limit', 100, type=int)
    if storage_manager:
        events = storage_manager.get_parquet_events(limit)
        return jsonify(events)
    return jsonify([])

@app.route('/api/duckdb_events')
def get_duckdb_events():
    limit = request.args.get('limit', 100, type=int)
    if storage_manager:
        events = storage_manager.get_duckdb_events(limit)
        return jsonify(events)
    return jsonify([])

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    return jsonify(recent_events.get(event_type, []))

@app.route('/api/etl_stats')
def get_etl_stats():
    if etl_pipeline:
        return jsonify(etl_pipeline.get_etl_stats())
    return jsonify({'error': 'ETL pipeline not available'})

@app.route('/api/snowflake_summary')
def get_snowflake_summary():
    if etl_pipeline:
        summary = etl_pipeline.get_snowflake_summary()
        return jsonify(summary) if summary else jsonify({'error': 'Unable to connect to Snowflake'})
    return jsonify({'error': 'ETL pipeline not available'})

@app.route('/api/run_etl', methods=['POST'])
def run_etl():
    """Run ETL pipeline manually"""
    if not etl_pipeline:
        return jsonify({'success': False, 'error': 'ETL pipeline not available'}), 500
    
    try:
        def run_etl_async():
            success = etl_pipeline.run_etl()
            if success:
                event_stats['etl_runs'] += 1
                logger.info("ETL pipeline completed successfully")
            else:
                logger.error("ETL pipeline failed")
        
        # Run ETL in background thread
        threading.Thread(target=run_etl_async, daemon=True).start()
        
        return jsonify({
            'success': True,
            'message': 'ETL pipeline started',
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/flush_storage', methods=['POST'])
def flush_storage():
    """Manually flush storage batches"""
    if not storage_manager:
        return jsonify({'success': False, 'error': 'Storage manager not available'}), 500
    
    try:
        results = storage_manager.flush_all_batches()
        return jsonify({
            'success': True,
            'results': results,
            'storage_stats': storage_manager.get_combined_stats()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# SocketIO event handlers
@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)
    if storage_manager:
        emit('storage_update', {
            'storage_stats': storage_manager.get_combined_stats()
        })

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start multi-storage event generation"""
    emit('monitoring_started', {'status': 'Multi-storage monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop multi-storage event generation"""
    emit('monitoring_stopped', {'status': 'Multi-storage monitoring stopped'})

@socketio.on('clear_stats')
def handle_clear_stats():
    """Clear all statistics"""
    global event_stats, recent_events
    
    event_stats.update({
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
        'last_raw_event': None,
        'last_clean_event': None,
        'data_quality_score': 100.0
    })
    
    recent_events = {
        'raw': [],
        'clean': [],
        'invalid': [],
        'parquet': [],
        'duckdb': []
    }
    
    emit('stats_cleared', {'status': 'Statistics cleared'})
    socketio.emit('stats_update', event_stats)

if __name__ == '__main__':
    logger.info("Starting Multi-Dashboard E-commerce Analytics...")
    logger.info("Features: Multi-storage (Parquet + DuckDB), ETL Pipeline, Snowflake Integration")
    logger.info("Dashboards available:")
    logger.info("  Main: http://localhost:5000")
    logger.info("  Parquet: http://localhost:5000/parquet")
    logger.info("  DuckDB: http://localhost:5000/duckdb")
    logger.info("  ETL: http://localhost:5000/etl")
    logger.info("  Snowflake: http://localhost:5000/snowflake")
    
    # Print available routes
    logger.info("Routes registered:")
    for rule in app.url_map.iter_rules():
        logger.info(f"  {rule}")
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)