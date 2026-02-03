from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
from datetime import datetime, timedelta
import logging
from data_generators import EcommerceDataGenerator
from multi_storage_manager import MultiStorageManager
from etl_pipeline import ETLPipeline

app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-dashboard-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize components
data_generator = EcommerceDataGenerator()
storage_manager = MultiStorageManager()
etl_pipeline = ETLPipeline()

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

def multi_storage_event_generator():
    """Generate events and store in Parquet/DuckDB randomly"""
    session_pool = {}
    
    while not stop_flags.get('multi_generator', False):
        try:
            # Session management
            if session_pool and random.random() < 0.7:
                session_id = random.choice(list(session_pool.keys()))
                session = session_pool[session_id]
            else:
                session = data_generator.generate_session_data()
                session_id = session['session_id']
                session_pool[session_id] = session
                event_stats['sessions_created'] += 1
                
                if len(session_pool) > 50:
                    oldest_session = min(session_pool.keys())
                    del session_pool[oldest_session]
            
            # Generate event
            event = data_generator.generate_random_event(session_id)
            event, is_valid = data_generator.add_data_quality_issues(event)
            
            # Store in multi-storage (Parquet or DuckDB randomly)
            storage_manager.add_event(event)
            
            # Create event data for UI
            raw_event_data = {
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'key': event.get('customer_id'),
                'value': event,
                'partition': 0,
                'offset': event_stats['raw_events_count'],
                'session_id': session_id,
                'storage_type': event.get('storage_type', 'unknown')
            }
            
            # Update statistics
            event_stats['raw_events_count'] += 1
            event_type = event.get('event_type', 'UNKNOWN')
            
            # Update storage-specific counters
            if event.get('storage_type') == 'parquet':
                event_stats['parquet_events'] += 1
                recent_events['parquet'].append(raw_event_data)
                if len(recent_events['parquet']) > 100:
                    recent_events['parquet'].pop(0)
            else:
                event_stats['duckdb_events'] += 1
                recent_events['duckdb'].append(raw_event_data)
                if len(recent_events['duckdb']) > 100:
                    recent_events['duckdb'].pop(0)
            
            # Update event type counters
            if event_type == 'PAGE_VIEW':
                event_stats['page_views'] += 1
            elif event_type in ['PRODUCT_VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'ADD_TO_WISHLIST']:
                event_stats['product_interactions'] += 1
            elif event_type == 'PURCHASE':
                event_stats['purchases'] += 1
            elif event_type == 'SEARCH':
                event_stats['searches'] += 1
            elif event_type in ['REVIEW', 'RATING', 'SHARE', 'NEWSLETTER_SIGNUP']:
                event_stats['user_engagement'] += 1
            
            # Add to recent events
            recent_events['raw'].append(raw_event_data)
            if len(recent_events['raw']) > 100:
                recent_events['raw'].pop(0)
            
            # Handle invalid events
            if not is_valid:
                event_stats['invalid_events_count'] += 1
                recent_events['invalid'].append(raw_event_data)
                if len(recent_events['invalid']) > 50:
                    recent_events['invalid'].pop(0)
            else:
                event_stats['clean_events_count'] += 1
                
                clean_event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': event.get('customer_id'),
                    'value': event,
                    'partition': 0,
                    'offset': event_stats['clean_events_count'],
                    'session_id': session_id,
                    'storage_type': event.get('storage_type', 'unknown')
                }
                
                recent_events['clean'].append(clean_event_data)
                if len(recent_events['clean']) > 100:
                    recent_events['clean'].pop(0)
                
                # Emit clean event
                socketio.emit('new_event', {
                    'type': 'clean',
                    'data': clean_event_data,
                    'stats': event_stats.copy()
                })
            
            # Update data quality score
            total_events = event_stats['raw_events_count']
            if total_events > 0:
                event_stats['data_quality_score'] = round(
                    (event_stats['clean_events_count'] / total_events) * 100, 2
                )
            
            event_stats['last_raw_event'] = raw_event_data
            
            # Emit raw event
            socketio.emit('new_event', {
                'type': 'raw',
                'data': raw_event_data,
                'stats': event_stats.copy()
            })
            
            # Emit storage-specific events
            socketio.emit('storage_event', {
                'type': event.get('storage_type', 'unknown'),
                'data': raw_event_data,
                'stats': storage_manager.get_combined_stats()
            })
            
            # Periodically flush batches
            if event_stats['raw_events_count'] % 100 == 0:
                threading.Thread(target=flush_storage_batches, daemon=True).start()
            
            # Variable delay based on event type
            if event_type == 'PAGE_VIEW':
                delay = random.uniform(0.1, 1.0)
            elif event_type == 'PURCHASE':
                delay = random.uniform(5.0, 15.0)
            else:
                delay = random.uniform(0.5, 3.0)
            
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error generating multi-storage event: {e}")
            time.sleep(1)

def flush_storage_batches():
    """Flush storage batches"""
    try:
        results = storage_manager.flush_all_batches()
        
        if results:
            logger.info(f"Flushed storage batches: {results}")
            
            # Emit storage update
            socketio.emit('storage_update', {
                'flush_results': results,
                'storage_stats': storage_manager.get_combined_stats()
            })
            
    except Exception as e:
        logger.error(f"Error flushing storage batches: {e}")

# Routes for different dashboards
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

# API Routes
@app.route('/api/stats')
def get_stats():
    return jsonify(event_stats)

@app.route('/api/storage_stats')
def get_storage_stats():
    return jsonify(storage_manager.get_combined_stats())

@app.route('/api/parquet_stats')
def get_parquet_stats():
    return jsonify(storage_manager.get_parquet_stats())

@app.route('/api/duckdb_stats')
def get_duckdb_stats():
    return jsonify(storage_manager.get_duckdb_stats())

@app.route('/api/parquet_events')
def get_parquet_events():
    limit = request.args.get('limit', 100, type=int)
    events = storage_manager.get_parquet_events(limit)
    return jsonify(events)

@app.route('/api/duckdb_events')
def get_duckdb_events():
    limit = request.args.get('limit', 100, type=int)
    events = storage_manager.get_duckdb_events(limit)
    return jsonify(events)

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    return jsonify(recent_events.get(event_type, []))

@app.route('/api/etl_stats')
def get_etl_stats():
    return jsonify(etl_pipeline.get_etl_stats())

@app.route('/api/snowflake_summary')
def get_snowflake_summary():
    summary = etl_pipeline.get_snowflake_summary()
    return jsonify(summary) if summary else jsonify({'error': 'Unable to connect to Snowflake'})

@app.route('/api/run_etl', methods=['POST'])
def run_etl():
    """Run ETL pipeline manually"""
    try:
        def run_etl_async():
            success = etl_pipeline.run_etl()
            if success:
                event_stats['etl_runs'] += 1
                socketio.emit('etl_completed', {
                    'success': True,
                    'stats': etl_pipeline.get_etl_stats(),
                    'timestamp': datetime.now().isoformat()
                })
            else:
                socketio.emit('etl_completed', {
                    'success': False,
                    'error': 'ETL pipeline failed',
                    'timestamp': datetime.now().isoformat()
                })
        
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
    try:
        results = storage_manager.flush_all_batches()
        return jsonify({
            'success': True,
            'results': results,
            'storage_stats': storage_manager.get_combined_stats()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# Socket.IO event handlers
@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)
    emit('storage_update', {
        'storage_stats': storage_manager.get_combined_stats()
    })

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start multi-storage event generation"""
    global threads, stop_flags
    
    if 'multi_generator' in stop_flags:
        stop_flags['multi_generator'] = True
    
    time.sleep(1)
    
    stop_flags['multi_generator'] = False
    threads['multi_generator'] = threading.Thread(target=multi_storage_event_generator)
    threads['multi_generator'].daemon = True
    threads['multi_generator'].start()
    
    emit('monitoring_started', {'status': 'Multi-storage monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop multi-storage event generation"""
    global stop_flags
    
    if 'multi_generator' in stop_flags:
        stop_flags['multi_generator'] = True
    
    threading.Thread(target=flush_storage_batches, daemon=True).start()
    
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
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)