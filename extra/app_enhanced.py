from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
from datetime import datetime, timedelta
import logging
from data_generators import EcommerceDataGenerator
from parquet_storage import ParquetStorageManager, SnowflakeParquetUploader

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-ui-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize data generator and storage
data_generator = EcommerceDataGenerator()
storage_manager = ParquetStorageManager()

# Snowflake configuration (update with your credentials)
SNOWFLAKE_CONFIG = {
    "user": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "COMPUTE_WH",
    "database": "KAFKA_DB",
    "schema": "STREAMING"
}

snowflake_uploader = SnowflakeParquetUploader(SNOWFLAKE_CONFIG)

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
    'last_raw_event': None,
    'last_clean_event': None,
    'parquet_files_created': 0,
    'snowflake_uploads': 0,
    'data_quality_score': 100.0
}

recent_events = {
    'raw': [],
    'clean': [],
    'invalid': [],
    'page_views': [],
    'purchases': [],
    'searches': []
}

# Thread control
threads = {}
stop_flags = {}
active_sessions = {}

def enhanced_event_generator():
    """Generate realistic e-commerce events with parquet storage"""
    session_pool = {}  # Keep track of active sessions
    
    while not stop_flags.get('enhanced_generator', False):
        try:
            # Decide whether to use existing session or create new one
            if session_pool and random.random() < 0.7:  # 70% chance to reuse session
                session_id = random.choice(list(session_pool.keys()))
                session = session_pool[session_id]
            else:
                # Create new session
                session = data_generator.generate_session_data()
                session_id = session['session_id']
                session_pool[session_id] = session
                event_stats['sessions_created'] += 1
                
                # Limit session pool size
                if len(session_pool) > 50:
                    oldest_session = min(session_pool.keys())
                    del session_pool[oldest_session]
            
            # Generate event based on realistic user journey
            event = data_generator.generate_random_event(session_id)
            
            # Add data quality issues
            event, is_valid = data_generator.add_data_quality_issues(event)
            
            # Create event data for UI
            raw_event_data = {
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'key': event.get('customer_id'),
                'value': event,
                'partition': 0,
                'offset': event_stats['raw_events_count'],
                'session_id': session_id
            }
            
            # Update statistics
            event_stats['raw_events_count'] += 1
            event_type = event.get('event_type', 'UNKNOWN')
            
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
            
            # Store in parquet (raw data)
            storage_manager.add_event(event, 'raw')
            
            # Add to recent events
            recent_events['raw'].append(raw_event_data)
            if len(recent_events['raw']) > 100:
                recent_events['raw'].pop(0)
            
            # Add to specific event type lists
            if event_type in recent_events:
                recent_events[event_type.lower()].append(raw_event_data)
                if len(recent_events[event_type.lower()]) > 50:
                    recent_events[event_type.lower()].pop(0)
            
            # Handle invalid events
            if not is_valid:
                event_stats['invalid_events_count'] += 1
                recent_events['invalid'].append(raw_event_data)
                if len(recent_events['invalid']) > 50:
                    recent_events['invalid'].pop(0)
                
                # Store failed events
                storage_manager.add_event(event, 'failed')
            else:
                # Process valid events
                event_stats['clean_events_count'] += 1
                
                clean_event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': event.get('customer_id'),
                    'value': event,
                    'partition': 0,
                    'offset': event_stats['clean_events_count'],
                    'session_id': session_id
                }
                
                recent_events['clean'].append(clean_event_data)
                if len(recent_events['clean']) > 100:
                    recent_events['clean'].pop(0)
                
                # Store processed events
                storage_manager.add_event(event, 'processed')
                
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
            
            # Periodically flush parquet batches and upload to Snowflake
            if event_stats['raw_events_count'] % 500 == 0:
                threading.Thread(target=flush_and_upload_data, daemon=True).start()
            
            # Variable delay based on event type (more realistic)
            if event_type == 'PAGE_VIEW':
                delay = random.uniform(0.1, 1.0)
            elif event_type == 'PURCHASE':
                delay = random.uniform(5.0, 15.0)  # Purchases are less frequent
            else:
                delay = random.uniform(0.5, 3.0)
            
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error generating enhanced event: {e}")
            time.sleep(1)

def flush_and_upload_data():
    """Flush parquet batches and upload to Snowflake"""
    try:
        # Flush all batches
        filepaths = storage_manager.flush_all_batches()
        
        if filepaths:
            event_stats['parquet_files_created'] += len(filepaths)
            logger.info(f"Created {len(filepaths)} parquet files")
            
            # Upload to Snowflake (only processed files)
            processed_files = [f for f in filepaths if 'processed' in str(f)]
            
            if processed_files and SNOWFLAKE_CONFIG['user'] != 'YOUR_USERNAME':
                results = snowflake_uploader.batch_upload_parquet_files(processed_files)
                successful_uploads = sum(1 for r in results if r['success'])
                event_stats['snowflake_uploads'] += successful_uploads
                
                logger.info(f"Uploaded {successful_uploads}/{len(processed_files)} files to Snowflake")
            
            # Emit storage update
            socketio.emit('storage_update', {
                'parquet_files': event_stats['parquet_files_created'],
                'snowflake_uploads': event_stats['snowflake_uploads'],
                'storage_stats': storage_manager.get_storage_stats()
            })
            
    except Exception as e:
        logger.error(f"Error in flush_and_upload_data: {e}")

@app.route('/')
def index():
    return render_template('enhanced_index.html')

@app.route('/api/stats')
def get_stats():
    return jsonify(event_stats)

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    return jsonify(recent_events.get(event_type, []))

@app.route('/api/storage_stats')
def get_storage_stats():
    return jsonify(storage_manager.get_storage_stats())

@app.route('/api/event_breakdown')
def get_event_breakdown():
    """Get breakdown of events by type"""
    breakdown = {
        'page_views': event_stats['page_views'],
        'product_interactions': event_stats['product_interactions'],
        'purchases': event_stats['purchases'],
        'searches': event_stats['searches'],
        'user_engagement': event_stats['user_engagement']
    }
    return jsonify(breakdown)

@app.route('/api/data_quality')
def get_data_quality():
    """Get data quality metrics"""
    quality_metrics = {
        'overall_score': event_stats['data_quality_score'],
        'valid_events': event_stats['clean_events_count'],
        'invalid_events': event_stats['invalid_events_count'],
        'total_events': event_stats['raw_events_count'],
        'success_rate': round((event_stats['clean_events_count'] / max(event_stats['raw_events_count'], 1)) * 100, 2)
    }
    return jsonify(quality_metrics)

@app.route('/api/flush_parquet', methods=['POST'])
def manual_flush_parquet():
    """Manually flush parquet batches"""
    try:
        filepaths = storage_manager.flush_all_batches()
        return jsonify({
            'success': True,
            'files_created': len(filepaths),
            'filepaths': [str(f) for f in filepaths]
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/upload_snowflake', methods=['POST'])
def manual_upload_snowflake():
    """Manually upload parquet files to Snowflake"""
    try:
        if SNOWFLAKE_CONFIG['user'] == 'YOUR_USERNAME':
            return jsonify({
                'success': False, 
                'error': 'Snowflake credentials not configured'
            }), 400
        
        # Get recent processed files
        processed_files = storage_manager.get_parquet_files('processed', days_back=1)
        
        if not processed_files:
            return jsonify({
                'success': False,
                'error': 'No processed parquet files found'
            }), 404
        
        results = snowflake_uploader.batch_upload_parquet_files(processed_files)
        successful_uploads = sum(1 for r in results if r['success'])
        
        return jsonify({
            'success': True,
            'files_processed': len(processed_files),
            'successful_uploads': successful_uploads,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)
    emit('storage_update', {
        'parquet_files': event_stats['parquet_files_created'],
        'snowflake_uploads': event_stats['snowflake_uploads'],
        'storage_stats': storage_manager.get_storage_stats()
    })

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start enhanced event generation"""
    global threads, stop_flags
    
    # Stop existing generator
    if 'enhanced_generator' in stop_flags:
        stop_flags['enhanced_generator'] = True
    
    # Wait for thread to stop
    time.sleep(1)
    
    # Start new generator
    stop_flags['enhanced_generator'] = False
    threads['enhanced_generator'] = threading.Thread(target=enhanced_event_generator)
    threads['enhanced_generator'].daemon = True
    threads['enhanced_generator'].start()
    
    emit('monitoring_started', {'status': 'Enhanced monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop enhanced event generation"""
    global stop_flags
    
    if 'enhanced_generator' in stop_flags:
        stop_flags['enhanced_generator'] = True
    
    # Flush remaining data
    threading.Thread(target=flush_and_upload_data, daemon=True).start()
    
    emit('monitoring_stopped', {'status': 'Enhanced monitoring stopped'})

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
        'last_raw_event': None,
        'last_clean_event': None,
        'data_quality_score': 100.0
    })
    
    recent_events = {
        'raw': [],
        'clean': [],
        'invalid': [],
        'page_views': [],
        'purchases': [],
        'searches': []
    }
    
    emit('stats_cleared', {'status': 'Statistics cleared'})
    socketio.emit('stats_update', event_stats)

if __name__ == '__main__':
    logger.info("Starting Enhanced Kafka Pipeline Dashboard...")
    logger.info("Features: Realistic data generation, Parquet storage, Snowflake integration")
    logger.info("Dashboard will be available at: http://localhost:5000")
    
    # Setup Snowflake objects if credentials are configured
    if SNOWFLAKE_CONFIG['user'] != 'YOUR_USERNAME':
        logger.info("Setting up Snowflake objects...")
        snowflake_uploader.setup_snowflake_objects()
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)