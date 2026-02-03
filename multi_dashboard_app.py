from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
from datetime import datetime

# Custom JSON encoder for Flask-SocketIO
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Monkey patch the json module to use our encoder
original_dumps = json.dumps
def patched_dumps(obj, **kwargs):
    kwargs.setdefault('cls', CustomJSONEncoder)
    return original_dumps(obj, **kwargs)

json.dumps = patched_dumps
import threading
import time
import random
from datetime import datetime, timedelta
import logging
import uuid

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def serialize_for_json(obj):
    """Recursively serialize all datetime objects and other non-JSON serializable objects"""
    if obj is None:
        return None
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [serialize_for_json(item) for item in obj]
    elif hasattr(obj, '__dict__'):
        return serialize_for_json(obj.__dict__)
    else:
        try:
            # Test if it's JSON serializable
            import json
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)  # Convert to string if not serializable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'multi-dashboard-secret'

# Initialize Kafka components (optional)
kafka_producer = None
kafka_consumer_raw = None
kafka_consumer_clean = None

try:
    from kafka import KafkaProducer, KafkaConsumer
    
    # Try to initialize Kafka components
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000
        )
        logger.info("Kafka Producer initialized successfully")
    except Exception as e:
        logger.warning(f"Kafka Producer not available: {e}")
        
    try:
        kafka_consumer_raw = KafkaConsumer(
            "raw_events",
            bootstrap_servers="localhost:9092",
            group_id="dashboard-consumer-raw",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        logger.info("Kafka Raw Consumer initialized successfully")
    except Exception as e:
        logger.warning(f"Kafka Raw Consumer not available: {e}")
        
    try:
        kafka_consumer_clean = KafkaConsumer(
            "clean_events",
            bootstrap_servers="localhost:9092",
            group_id="dashboard-consumer-clean",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=1000
        )
        logger.info("Kafka Clean Consumer initialized successfully")
    except Exception as e:
        logger.warning(f"Kafka Clean Consumer not available: {e}")
        
except ImportError:
    logger.warning("kafka-python not installed, Kafka features disabled")

# Initialize other components
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

# Enhanced stats for Kafka integration
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
    'kafka_raw_events': 0,
    'kafka_clean_events': 0,
    'data_quality_score': 100.0,
    'etl_runs': 0,
    'snowflake_records': 0
}

recent_events = {
    'raw': [],
    'clean': [],
    'invalid': [],
    'parquet': [],
    'duckdb': [],
    'kafka_raw': [],
    'kafka_clean': []
}

# Thread control
threads = {}
stop_flags = {}

# Kafka event generation functions
def generate_kafka_event():
    """Generate realistic e-commerce event for Kafka"""
    event_types = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "PURCHASE", "SEARCH", "REVIEW"]
    is_invalid = random.random() < 0.15
    
    customer_id = f"CUST_{random.randint(1, 100)}"
    event_type = random.choice(event_types)
    amount = round(random.uniform(10, 500), 2)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id if not is_invalid else None,
        "event_type": event_type,
        "amount": amount if event_type in ["ADD_TO_CART", "PURCHASE"] else 0,
        "currency": "USD",
        "event_timestamp": datetime.utcnow().isoformat(),
        "is_valid": not is_invalid,
        "source": "dashboard_producer"
    }
    
    return customer_id, event

def kafka_producer_thread():
    """Kafka producer thread"""
    while not stop_flags.get('kafka_producer', False) and kafka_producer:
        try:
            key, event = generate_kafka_event()
            
            kafka_producer.send("raw_events", key=key, value=event)
            
            event_stats['kafka_raw_events'] += 1
            event_stats['raw_events_count'] += 1
            
            # Add to recent events
            kafka_event_data = {
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'key': key,
                'value': serialize_for_json(event),
                'topic': 'raw_events'
            }
            
            recent_events['kafka_raw'].append(kafka_event_data)
            if len(recent_events['kafka_raw']) > 100:
                recent_events['kafka_raw'].pop(0)
            
            # Emit to WebSocket
            if socketio:
                # Ensure all data is JSON serializable
                clean_data = {
                    'type': 'raw',
                    'data': {
                        'timestamp': kafka_event_data['timestamp'],
                        'key': kafka_event_data['key'],
                        'value': serialize_for_json(kafka_event_data['value']),
                        'topic': kafka_event_data['topic']
                    },
                    'stats': serialize_for_json(event_stats.copy())
                }
                socketio.emit('kafka_raw_event', clean_data)
            
            time.sleep(random.uniform(1.0, 3.0))
            
        except Exception as e:
            logger.error(f"Error in Kafka producer thread: {e}")
            time.sleep(5)

def kafka_consumer_thread():
    """Kafka consumer thread for raw events"""
    while not stop_flags.get('kafka_consumer', False) and kafka_consumer_raw:
        try:
            for message in kafka_consumer_raw:
                if stop_flags.get('kafka_consumer', False):
                    break
                
                key = message.key
                event = message.value
                
                event_stats['raw_events_count'] += 1
                
                # Process for storage
                if storage_manager and data_generator:
                    storage_manager.add_event(event)
                    
                    # Update event type counters
                    event_type = event.get('event_type', 'UNKNOWN')
                    if event_type == 'PAGE_VIEW':
                        event_stats['page_views'] += 1
                    elif event_type in ['PRODUCT_VIEW', 'ADD_TO_CART']:
                        event_stats['product_interactions'] += 1
                    elif event_type == 'PURCHASE':
                        event_stats['purchases'] += 1
                    elif event_type == 'SEARCH':
                        event_stats['searches'] += 1
                    elif event_type == 'REVIEW':
                        event_stats['user_engagement'] += 1
                
                # Add to recent events
                raw_event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': key,
                    'value': serialize_for_json(event),
                    'topic': 'raw_events',
                    'partition': message.partition,
                    'offset': message.offset
                }
                
                recent_events['raw'].append(raw_event_data)
                if len(recent_events['raw']) > 100:
                    recent_events['raw'].pop(0)
                
                # Emit to WebSocket
                if socketio:
                    # Ensure all data is JSON serializable
                    clean_data = {
                        'type': 'raw',
                        'data': {
                            'timestamp': raw_event_data['timestamp'],
                            'key': raw_event_data['key'],
                            'value': serialize_for_json(raw_event_data['value']),
                            'topic': raw_event_data['topic'],
                            'partition': raw_event_data['partition'],
                            'offset': raw_event_data['offset']
                        },
                        'stats': serialize_for_json(event_stats.copy())
                    }
                    socketio.emit('kafka_raw_event', clean_data)
                
        except Exception as e:
            logger.error(f"Error in Kafka consumer thread: {e}")
            time.sleep(5)

# Routes for different dashboards
@app.route('/')
def main_dashboard():
    logger.info("Main dashboard accessed")
    return render_template('main_dashboard.html')

@app.route('/kafka')
def kafka_dashboard():
    logger.info("Kafka dashboard accessed")
    return render_template('kafka_dashboard.html')

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
    return jsonify(serialize_for_json(event_stats))

@app.route('/api/kafka_stats')
def get_kafka_stats():
    return jsonify({
        'raw_events': event_stats['kafka_raw_events'],
        'clean_events': event_stats['kafka_clean_events'],
        'producer_active': not stop_flags.get('kafka_producer', False),
        'consumer_active': not stop_flags.get('kafka_consumer', False),
        'kafka_available': kafka_producer is not None,
        'topics': {
            'raw_topic': 'raw_events',
            'clean_topic': 'clean_events'
        },
        'bootstrap_servers': 'localhost:9092'
    })

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    events = recent_events.get(event_type, [])
    return jsonify(serialize_for_json(events))

@app.route('/api/storage_stats')
def get_storage_stats():
    if storage_manager:
        return jsonify(storage_manager.get_combined_stats())
    return jsonify({'error': 'Storage manager not available'})

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

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# SocketIO event handlers
@socketio.on('connect')
def handle_connect():
    logger.info('Client connected')
    try:
        emit('stats_update', serialize_for_json(event_stats))
        if storage_manager:
            emit('storage_update', {
                'storage_stats': serialize_for_json(storage_manager.get_combined_stats())
            })
        # Send connection confirmation
        emit('connection_confirmed', {
            'status': 'connected',
            'timestamp': datetime.now().isoformat(),
            'kafka_available': kafka_producer is not None
        })
    except Exception as e:
        logger.error(f"Error in connect handler: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_kafka_producer')
def handle_start_kafka_producer():
    """Start Kafka producer"""
    if not kafka_producer:
        emit('kafka_error', {'error': 'Kafka not available'})
        return
        
    global threads, stop_flags
    
    if 'kafka_producer' in stop_flags:
        stop_flags['kafka_producer'] = True
    
    time.sleep(1)
    
    stop_flags['kafka_producer'] = False
    threads['kafka_producer'] = threading.Thread(target=kafka_producer_thread)
    threads['kafka_producer'].daemon = True
    threads['kafka_producer'].start()
    
    emit('kafka_producer_started', {'status': 'Kafka producer started'})

@socketio.on('stop_kafka_producer')
def handle_stop_kafka_producer():
    """Stop Kafka producer"""
    global stop_flags
    
    if 'kafka_producer' in stop_flags:
        stop_flags['kafka_producer'] = True
    
    emit('kafka_producer_stopped', {'status': 'Kafka producer stopped'})

@socketio.on('start_kafka_consumers')
def handle_start_kafka_consumers():
    """Start Kafka consumers"""
    if not kafka_consumer_raw:
        emit('kafka_error', {'error': 'Kafka consumers not available'})
        return
        
    global threads, stop_flags
    
    if 'kafka_consumer' in stop_flags:
        stop_flags['kafka_consumer'] = True
    
    time.sleep(1)
    
    stop_flags['kafka_consumer'] = False
    threads['kafka_consumer'] = threading.Thread(target=kafka_consumer_thread)
    threads['kafka_consumer'].daemon = True
    threads['kafka_consumer'].start()
    
    emit('kafka_consumers_started', {'status': 'Kafka consumers started'})

@socketio.on('stop_kafka_consumers')
def handle_stop_kafka_consumers():
    """Stop Kafka consumers"""
    global stop_flags
    
    if 'kafka_consumer' in stop_flags:
        stop_flags['kafka_consumer'] = True
    
    emit('kafka_consumers_stopped', {'status': 'Kafka consumers stopped'})

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
        'kafka_raw_events': 0,
        'kafka_clean_events': 0,
        'data_quality_score': 100.0
    })
    
    recent_events = {
        'raw': [],
        'clean': [],
        'invalid': [],
        'parquet': [],
        'duckdb': [],
        'kafka_raw': [],
        'kafka_clean': []
    }
    
    emit('stats_cleared', {'status': 'Statistics cleared'})
    socketio.emit('stats_update', serialize_for_json(event_stats))

@socketio.on('start_monitoring')
def handle_start_monitoring():
    """Start basic monitoring without Kafka"""
    global threads, stop_flags
    
    if 'basic_generator' in stop_flags:
        stop_flags['basic_generator'] = True
    
    time.sleep(1)
    
    stop_flags['basic_generator'] = False
    threads['basic_generator'] = threading.Thread(target=basic_event_generator)
    threads['basic_generator'].daemon = True
    threads['basic_generator'].start()
    
    emit('monitoring_started', {'status': 'Basic monitoring started'})

@socketio.on('stop_monitoring')
def handle_stop_monitoring():
    """Stop basic monitoring"""
    global stop_flags
    
    if 'basic_generator' in stop_flags:
        stop_flags['basic_generator'] = True
    
    emit('monitoring_stopped', {'status': 'Basic monitoring stopped'})

def basic_event_generator():
    """Basic event generator for testing without Kafka"""
    while not stop_flags.get('basic_generator', False):
        try:
            if data_generator and storage_manager:
                # Generate session
                session = data_generator.generate_session_data()
                session_id = session['session_id']
                
                # Generate event
                event = data_generator.generate_random_event(session_id)
                event, is_valid = data_generator.add_data_quality_issues(event)
                
                # Store in multi-storage
                storage_manager.add_event(event)
                
                # Update stats
                event_stats['raw_events_count'] += 1
                if is_valid:
                    event_stats['clean_events_count'] += 1
                else:
                    event_stats['invalid_events_count'] += 1
                
                # Update event type counters
                event_type = event.get('event_type', 'UNKNOWN')
                if event_type == 'PAGE_VIEW':
                    event_stats['page_views'] += 1
                elif event_type in ['PRODUCT_VIEW', 'ADD_TO_CART']:
                    event_stats['product_interactions'] += 1
                elif event_type == 'PURCHASE':
                    event_stats['purchases'] += 1
                elif event_type == 'SEARCH':
                    event_stats['searches'] += 1
                elif event_type == 'REVIEW':
                    event_stats['user_engagement'] += 1
                
                # Create event data for UI
                event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': event.get('customer_id'),
                    'value': serialize_for_json(event),
                    'partition': 0,
                    'offset': event_stats['raw_events_count']
                }
                
                recent_events['raw'].append(event_data)
                if len(recent_events['raw']) > 100:
                    recent_events['raw'].pop(0)
                
                # Emit to WebSocket
                clean_data = {
                    'type': 'raw',
                    'data': {
                        'timestamp': event_data['timestamp'],
                        'key': event_data['key'],
                        'value': serialize_for_json(event_data['value']),
                        'partition': event_data['partition'],
                        'offset': event_data['offset']
                    },
                    'stats': serialize_for_json(event_stats.copy())
                }
                socketio.emit('new_event', clean_data)
                
                # Variable delay
                time.sleep(random.uniform(1.0, 3.0))
            else:
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error in basic event generator: {e}")
            time.sleep(5)

if __name__ == '__main__':
    logger.info("Starting Multi-Dashboard E-commerce Analytics...")
    logger.info("Features: Multi-storage (Parquet + DuckDB), ETL Pipeline, Snowflake Integration")
    if kafka_producer:
        logger.info("Kafka Integration: ENABLED")
        
        # Auto-start Kafka consumer thread
        logger.info("Auto-starting Kafka consumer thread...")
        stop_flags['kafka_consumer'] = False
        threads['kafka_consumer'] = threading.Thread(target=kafka_consumer_thread)
        threads['kafka_consumer'].daemon = True
        threads['kafka_consumer'].start()
        logger.info("Kafka consumer thread started successfully")
    else:
        logger.info("Kafka Integration: DISABLED (Kafka not available)")
    
    logger.info("Dashboards available:")
    logger.info("  Main: http://localhost:5004")
    if kafka_producer:
        logger.info("  Kafka: http://localhost:5004/kafka")
    logger.info("  Parquet: http://localhost:5004/parquet")
    logger.info("  DuckDB: http://localhost:5004/duckdb")
    logger.info("  ETL: http://localhost:5004/etl")
    logger.info("  Snowflake: http://localhost:5004/snowflake")
    
    # Print available routes
    logger.info("Routes registered:")
    for rule in app.url_map.iter_rules():
        logger.info(f"  {rule}")
    
    socketio.run(app, debug=True, host='0.0.0.0', port=5004)
