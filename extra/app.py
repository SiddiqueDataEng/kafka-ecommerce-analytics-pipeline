from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import queue
import logging

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-ui-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "raw_events"
CLEAN_TOPIC = "clean_events"

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

def monitor_topic(topic_name, event_type):
    """Monitor a Kafka topic and emit events to the UI"""
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'ui-monitor-{event_type}',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=1000
        )
        
        logger.info(f"Started monitoring {topic_name}")
        
        while not stop_flags.get(f'monitor_{event_type}', False):
            try:
                message_pack = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        event_data = {
                            'timestamp': datetime.now().strftime('%H:%M:%S'),
                            'key': message.key,
                            'value': message.value,
                            'partition': message.partition,
                            'offset': message.offset
                        }
                        
                        # Update stats
                        if event_type == 'raw':
                            event_stats['raw_events_count'] += 1
                            event_stats['last_raw_event'] = event_data
                            if message.value and not message.value.get('is_valid', True):
                                event_stats['invalid_events_count'] += 1
                                recent_events['invalid'].append(event_data)
                                if len(recent_events['invalid']) > 50:
                                    recent_events['invalid'].pop(0)
                        elif event_type == 'clean':
                            event_stats['clean_events_count'] += 1
                            event_stats['last_clean_event'] = event_data
                        
                        # Keep recent events
                        recent_events[event_type].append(event_data)
                        if len(recent_events[event_type]) > 50:
                            recent_events[event_type].pop(0)
                        
                        # Emit to UI
                        socketio.emit('new_event', {
                            'type': event_type,
                            'data': event_data,
                            'stats': event_stats
                        })
                        
            except Exception as e:
                if not stop_flags.get(f'monitor_{event_type}', False):
                    logger.error(f"Error monitoring {topic_name}: {e}")
                time.sleep(1)
                
        consumer.close()
        logger.info(f"Stopped monitoring {topic_name}")
        
    except Exception as e:
        logger.error(f"Failed to start monitoring {topic_name}: {e}")

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
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        topics = admin_client.list_topics()
        return jsonify(list(topics))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    emit('stats_update', event_stats)

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start monitoring both topics"""
    global threads, stop_flags
    
    # Stop existing monitors
    for key in list(stop_flags.keys()):
        if key.startswith('monitor_'):
            stop_flags[key] = True
    
    # Wait for threads to stop
    time.sleep(2)
    
    # Clear stop flags
    stop_flags = {}
    
    # Start new monitoring threads
    for topic, event_type in [(RAW_TOPIC, 'raw'), (CLEAN_TOPIC, 'clean')]:
        thread_key = f'monitor_{event_type}'
        if thread_key not in threads or not threads[thread_key].is_alive():
            stop_flags[thread_key] = False
            threads[thread_key] = threading.Thread(
                target=monitor_topic, 
                args=(topic, event_type)
            )
            threads[thread_key].daemon = True
            threads[thread_key].start()
    
    emit('monitoring_started', {'status': 'Monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop all monitoring"""
    global stop_flags
    
    for key in list(stop_flags.keys()):
        if key.startswith('monitor_'):
            stop_flags[key] = True
    
    emit('monitoring_stopped', {'status': 'Monitoring stopped'})

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
    # Start monitoring on startup
    for topic, event_type in [(RAW_TOPIC, 'raw'), (CLEAN_TOPIC, 'clean')]:
        thread_key = f'monitor_{event_type}'
        stop_flags[thread_key] = False
        threads[thread_key] = threading.Thread(
            target=monitor_topic, 
            args=(topic, event_type)
        )
        threads[thread_key].daemon = True
        threads[thread_key].start()
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)