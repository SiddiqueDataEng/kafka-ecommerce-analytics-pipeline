from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
from datetime import datetime, timedelta
import logging
import os
from data_generators import EcommerceDataGenerator

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-ui-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize data generator
data_generator = EcommerceDataGenerator()

# Simple storage without pandas
class SimpleStorageManager:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        os.makedirs(f"{base_path}/raw", exist_ok=True)
        os.makedirs(f"{base_path}/processed", exist_ok=True)
        os.makedirs(f"{base_path}/failed", exist_ok=True)
        
        self.batch_size = 1000
        self.current_batches = {
            'raw': [],
            'processed': [],
            'failed': []
        }
    
    def add_event(self, event, category='raw'):
        self.current_batches[category].append(event)
        
        if len(self.current_batches[category]) >= self.batch_size:
            self.flush_batch(category)
    
    def flush_batch(self, category='raw'):
        if not self.current_batches[category]:
            return None
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{category}_events_{timestamp}_{len(self.current_batches[category])}.json"
            filepath = os.path.join(self.base_path, category, filename)
            
            with open(filepath, 'w') as f:
                json.dump(self.current_batches[category], f, indent=2)
            
            logger.info(f"Saved {len(self.current_batches[category])} events to {filepath}")
            
            self.current_batches[category] = []
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
            return None
    
    def get_storage_stats(self):
        stats = {}
        for category in ['raw', 'processed', 'failed']:
            path = os.path.join(self.base_path, category)
            files = [f for f in os.listdir(path) if f.endswith('.json')]
            total_size = sum(os.path.getsize(os.path.join(path, f)) for f in files)
            
            stats[category] = {
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'latest_file': max(files, key=lambda x: os.path.getmtime(os.path.join(path, x))) if files else None
            }
        
        return stats

storage_manager = SimpleStorageManager()

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
    'json_files_created': 0,
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
    """Generate realistic e-commerce events with JSON storage"""
    session_pool = {}
    
    while not stop_flags.get('enhanced_generator', False):
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
            
            # Store in JSON
            storage_manager.add_event(event, 'raw')
            
            # Add to recent events
            recent_events['raw'].append(raw_event_data)
            if len(recent_events['raw']) > 100:
                recent_events['raw'].pop(0)
            
            # Add to specific event type lists
            if event_type.lower() in recent_events:
                recent_events[event_type.lower()].append(raw_event_data)
                if len(recent_events[event_type.lower()]) > 50:
                    recent_events[event_type.lower()].pop(0)
            
            # Handle invalid events
            if not is_valid:
                event_stats['invalid_events_count'] += 1
                recent_events['invalid'].append(raw_event_data)
                if len(recent_events['invalid']) > 50:
                    recent_events['invalid'].pop(0)
                
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
            
            # Periodically flush batches
            if event_stats['raw_events_count'] % 500 == 0:
                threading.Thread(target=flush_data, daemon=True).start()
            
            # Variable delay based on event type
            if event_type == 'PAGE_VIEW':
                delay = random.uniform(0.1, 1.0)
            elif event_type == 'PURCHASE':
                delay = random.uniform(5.0, 15.0)
            else:
                delay = random.uniform(0.5, 3.0)
            
            time.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error generating enhanced event: {e}")
            time.sleep(1)

def flush_data():
    """Flush JSON batches"""
    try:
        files_created = 0
        for category in ['raw', 'processed', 'failed']:
            filepath = storage_manager.flush_batch(category)
            if filepath:
                files_created += 1
        
        if files_created > 0:
            event_stats['json_files_created'] += files_created
            logger.info(f"Created {files_created} JSON files")
            
            socketio.emit('storage_update', {
                'json_files': event_stats['json_files_created'],
                'storage_stats': storage_manager.get_storage_stats()
            })
            
    except Exception as e:
        logger.error(f"Error in flush_data: {e}")

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
    quality_metrics = {
        'overall_score': event_stats['data_quality_score'],
        'valid_events': event_stats['clean_events_count'],
        'invalid_events': event_stats['invalid_events_count'],
        'total_events': event_stats['raw_events_count'],
        'success_rate': round((event_stats['clean_events_count'] / max(event_stats['raw_events_count'], 1)) * 100, 2)
    }
    return jsonify(quality_metrics)

@app.route('/api/flush_json', methods=['POST'])
def manual_flush_json():
    try:
        files_created = 0
        filepaths = []
        for category in ['raw', 'processed', 'failed']:
            filepath = storage_manager.flush_batch(category)
            if filepath:
                files_created += 1
                filepaths.append(filepath)
        
        return jsonify({
            'success': True,
            'files_created': files_created,
            'filepaths': filepaths
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)
    emit('storage_update', {
        'json_files': event_stats['json_files_created'],
        'storage_stats': storage_manager.get_storage_stats()
    })

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    global threads, stop_flags
    
    if 'enhanced_generator' in stop_flags:
        stop_flags['enhanced_generator'] = True
    
    time.sleep(1)
    
    stop_flags['enhanced_generator'] = False
    threads['enhanced_generator'] = threading.Thread(target=enhanced_event_generator)
    threads['enhanced_generator'].daemon = True
    threads['enhanced_generator'].start()
    
    emit('monitoring_started', {'status': 'Enhanced monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    global stop_flags
    
    if 'enhanced_generator' in stop_flags:
        stop_flags['enhanced_generator'] = True
    
    threading.Thread(target=flush_data, daemon=True).start()
    
    emit('monitoring_stopped', {'status': 'Enhanced monitoring stopped'})

@socketio.on('clear_stats')
def handle_clear_stats():
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
    logger.info("Starting Enhanced E-commerce Analytics Dashboard...")
    logger.info("Features: Realistic data generation, JSON storage, Advanced analytics")
    logger.info("Dashboard will be available at: http://localhost:5000")
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)