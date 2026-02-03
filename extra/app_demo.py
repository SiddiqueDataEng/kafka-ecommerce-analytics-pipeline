from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
import uuid
from datetime import datetime, timedelta
import logging

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-ui-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for monitoring
event_stats = {
    'raw_events_count': 0,
    'clean_events_count': 0,
    'invalid_events_count': 0,
    'last_raw_event': None,
    'last_clean_event': None,
    'producer_running': False,
    'processor_running': False,
    'consumer_running': False
}

recent_events = {
    'raw': [],
    'clean': [],
    'invalid': []
}

# Thread control
threads = {}
stop_flags = {}

# Demo data generators
EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past = now - timedelta(days=6)
    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_demo_event():
    """Generate a demo event similar to the producer"""
    is_invalid = random.random() < 0.25
    
    customer_id = f"CUST_{random.randint(1,5)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10,500),2)
    currency = "USD"
    
    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id",
            "event_type", 
            "amount",
            "currency"
        ])
    
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": None if invalid_field == "customer_id" else customer_id,
        "event_type": (
            random.choice(INVALID_EVENT_TYPES)
            if invalid_field == "event_type"
            else event_type
        ),
        "amount": (
            random.uniform(-500, -10)
            if invalid_field == "amount"
            else amount
        ),
        "currency": None if invalid_field == "currency" else currency,
        "event_timestamp": random_timestamp_last_6_days().isoformat(),
        "is_valid": not is_invalid,
        "invalid_field": invalid_field
    }
    
    return customer_id, event

def is_valid_event(event):
    """Validate event similar to stream processor"""
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in EVENT_TYPES:
        return False
    if event.get("amount") is None or event.get("amount") <= 0:
        return False
    if not event.get("currency"):
        return False
    if event.get("is_valid") is not True:
        return False
    return True

def demo_event_generator():
    """Generate demo events continuously"""
    while not stop_flags.get('demo_generator', False):
        try:
            # Generate raw event
            key, raw_event = generate_demo_event()
            
            raw_event_data = {
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'key': key,
                'value': raw_event,
                'partition': 0,
                'offset': event_stats['raw_events_count']
            }
            
            # Update raw stats
            event_stats['raw_events_count'] += 1
            event_stats['last_raw_event'] = raw_event_data
            
            # Add to recent events
            recent_events['raw'].append(raw_event_data)
            if len(recent_events['raw']) > 50:
                recent_events['raw'].pop(0)
            
            # Check if invalid
            if not raw_event.get('is_valid', True):
                event_stats['invalid_events_count'] += 1
                recent_events['invalid'].append(raw_event_data)
                if len(recent_events['invalid']) > 50:
                    recent_events['invalid'].pop(0)
            
            # Emit raw event
            socketio.emit('new_event', {
                'type': 'raw',
                'data': raw_event_data,
                'stats': event_stats.copy()
            })
            
            # Process event (simulate stream processor)
            if is_valid_event(raw_event):
                clean_event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': key,
                    'value': raw_event,
                    'partition': 0,
                    'offset': event_stats['clean_events_count']
                }
                
                # Update clean stats
                event_stats['clean_events_count'] += 1
                event_stats['last_clean_event'] = clean_event_data
                
                # Add to recent events
                recent_events['clean'].append(clean_event_data)
                if len(recent_events['clean']) > 50:
                    recent_events['clean'].pop(0)
                
                # Emit clean event
                socketio.emit('new_event', {
                    'type': 'clean',
                    'data': clean_event_data,
                    'stats': event_stats.copy()
                })
            
            # Wait before next event
            time.sleep(random.uniform(0.5, 2.0))
            
        except Exception as e:
            logger.error(f"Error generating demo event: {e}")
            time.sleep(1)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/stats')
def get_stats():
    return jsonify(event_stats)

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    return jsonify(recent_events.get(event_type, []))

@app.route('/api/topics')
def get_topics():
    return jsonify(['raw_events', 'clean_events'])

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start demo event generation"""
    global threads, stop_flags
    
    # Stop existing generator
    if 'demo_generator' in stop_flags:
        stop_flags['demo_generator'] = True
    
    # Wait for thread to stop
    time.sleep(1)
    
    # Start new generator
    stop_flags['demo_generator'] = False
    threads['demo_generator'] = threading.Thread(target=demo_event_generator)
    threads['demo_generator'].daemon = True
    threads['demo_generator'].start()
    
    emit('monitoring_started', {'status': 'Demo monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop demo event generation"""
    global stop_flags
    
    if 'demo_generator' in stop_flags:
        stop_flags['demo_generator'] = True
    
    emit('monitoring_stopped', {'status': 'Demo monitoring stopped'})

@socketio.on('clear_stats')
def handle_clear_stats():
    """Clear all statistics"""
    global event_stats, recent_events
    
    event_stats.update({
        'raw_events_count': 0,
        'clean_events_count': 0,
        'invalid_events_count': 0,
        'last_raw_event': None,
        'last_clean_event': None
    })
    
    recent_events = {
        'raw': [],
        'clean': [],
        'invalid': []
    }
    
    emit('stats_cleared', {'status': 'Statistics cleared'})
    socketio.emit('stats_update', event_stats)

if __name__ == '__main__':
    logger.info("Starting Kafka Pipeline Demo Dashboard...")
    logger.info("Dashboard will be available at: http://localhost:5000")
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)