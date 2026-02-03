from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
import time
import random
from datetime import datetime, timedelta
import logging
from kafka import KafkaProducer, KafkaConsumer
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-multi-dashboard-secret'

# Kafka Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "raw_events"
CLEAN_TOPIC = "clean_events"
CONSUMER_GROUP = "dashboard-consumer"

# Initialize components with error handling
data_generator = None
storage_manager = None
etl_pipeline = None
kafka_producer = None
kafka_consumer_raw = None
kafka_consumer_clean = None

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

# Initialize Kafka components
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Kafka Producer initialized successfully")
except Exception as e:
    logger.error(f"Error initializing Kafka Producer: {e}")

try:
    kafka_consumer_raw = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"{CONSUMER_GROUP}-raw",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    logger.info("Kafka Raw Consumer initialized successfully")
except Exception as e:
    logger.error(f"Error initializing Kafka Raw Consumer: {e}")

try:
    kafka_consumer_clean = KafkaConsumer(
        CLEAN_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"{CONSUMER_GROUP}-clean",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    logger.info("Kafka Clean Consumer initialized successfully")
except Exception as e:
    logger.error(f"Error initializing Kafka Clean Consumer: {e}")

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
    'kafka_raw_events': 0,
    'kafka_clean_events': 0,
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
    'duckdb': [],
    'kafka_raw': [],
    'kafka_clean': []
}

# Thread control
threads = {}
stop_flags = {}

# Kafka Event Generation Functions
EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE", "PRODUCT_VIEW", "SEARCH", "REVIEW"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def generate_kafka_event():
    """Generate realistic e-commerce event for Kafka"""
    is_invalid = random.random() < 0.15
    
    customer_id = f"CUST_{random.randint(1, 100)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10, 500), 2)
    currency = "USD"
    
    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id", "event_type", "amount", "currency"
        ])
    
    # Generate realistic event data based on type
    event_data = {}
    if event_type == "PAGE_VIEW":
        event_data = {
            "page_url": f"/products/{random.randint(1, 1000)}",
            "referrer": random.choice(["google.com", "facebook.com", "direct", "email"]),
            "session_duration": random.randint(30, 300)
        }
    elif event_type == "PRODUCT_VIEW":
        event_data = {
            "product_id": f"PROD_{random.randint(1, 500)}",
            "category": random.choice(["electronics", "clothing", "books", "home"]),
            "price": amount
        }
    elif event_type == "ADD_TO_CART":
        event_data = {
            "product_id": f"PROD_{random.randint(1, 500)}",
            "quantity": random.randint(1, 5),
            "price": amount
        }
    elif event_type == "PURCHASE":
        event_data = {
            "order_id": f"ORDER_{random.randint(10000, 99999)}",
            "total_amount": amount,
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay"])
        }
    elif event_type == "SEARCH":
        event_data = {
            "search_query": random.choice(["laptop", "shoes", "book", "phone", "headphones"]),
            "results_count": random.randint(0, 100)
        }
    elif event_type == "REVIEW":
        event_data = {
            "product_id": f"PROD_{random.randint(1, 500)}",
            "rating": random.randint(1, 5),
            "review_length": random.randint(50, 500)
        }
    
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
        "event_timestamp": datetime.utcnow().isoformat(),
        "event_data": event_data,
        "is_valid": not is_invalid,
        "invalid_field": invalid_field,
        "source": "kafka_producer"
    }
    
    return customer_id, event

def kafka_producer_thread():
    """Kafka producer thread for generating events"""
    logger.info("Starting Kafka producer thread...")
    
    while not stop_flags.get('kafka_producer', False):
        try:
            if kafka_producer:
                key, event = generate_kafka_event()
                
                # Send to Kafka
                kafka_producer.send(
                    topic=RAW_TOPIC,
                    key=key,
                    value=event
                )
                
                # Update stats
                event_stats['kafka_raw_events'] += 1
                event_stats['raw_events_count'] += 1
                
                # Add to recent events
                kafka_event_data = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'key': key,
                    'value': event,
                    'topic': RAW_TOPIC,
                    'partition': 0,
                    'offset': event_stats['kafka_raw_events']
                }
                
                recent_events['kafka_raw'].append(kafka_event_data)
                if len(recent_events['kafka_raw']) > 100:
                    recent_events['kafka_raw'].pop(0)
                
                logger.info(f"Produced Kafka event | key={key} | type={event['event_type']} | valid={event['is_valid']}")
                
                # Variable delay based on event type
                if event['event_type'] == 'PAGE_VIEW':
                    delay = random.uniform(0.5, 2.0)
                elif event['event_type'] == 'PURCHASE':
                    delay = random.uniform(10.0, 30.0)
                else:
                    delay = random.uniform(1.0, 5.0)
                
                time.sleep(delay)
            else:
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error in Kafka producer thread: {e}")
            time.sleep(5)

def kafka_consumer_raw_thread():
    """Kafka consumer thread for raw events"""
    logger.info("Starting Kafka raw consumer thread...")
    
    while not stop_flags.get('kafka_consumer_raw', False):
        try:
            if kafka_consumer_raw:
                for message in kafka_consumer_raw:
                    if stop_flags.get('kafka_consumer_raw', False):
                        break
                    
                    key = message.key
                    event = message.value
                    
                    # Update stats
                    event_stats['raw_events_count'] += 1
                    
                    # Process event for storage
                    if storage_manager and data_generator:
                        # Add to multi-storage system
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
                    
                    # Create event data for UI
                    raw_event_data = {
                        'timestamp': datetime.now().strftime('%H:%M:%S'),
                        'key': key,
                        'value': event,
                        'topic': RAW_TOPIC,
                        'partition': message.partition,
                        'offset': message.offset
                    }
                    
                    recent_events['raw'].append(raw_event_data)
                    if len(recent_events['raw']) > 100:
                        recent_events['raw'].pop(0)
                    
                    # Emit to WebSocket
                    if socketio:
                        socketio.emit('kafka_raw_event', {
                            'type': 'raw',
                            'data': raw_event_data,
                            'stats': event_stats.copy()
                        })
                    
                    logger.info(f"Consumed raw event | key={key} | type={event.get('event_type')} | partition={message.partition} | offset={message.offset}")
            else:
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error in Kafka raw consumer thread: {e}")
            time.sleep(5)

def kafka_consumer_clean_thread():
    """Kafka consumer thread for clean events"""
    logger.info("Starting Kafka clean consumer thread...")
    
    while not stop_flags.get('kafka_consumer_clean', False):
        try:
            if kafka_consumer_clean:
                for message in kafka_consumer_clean:
                    if stop_flags.get('kafka_consumer_clean', False):
                        break
                    
                    key = message.key
                    event = message.value
                    
                    # Update stats
                    event_stats['kafka_clean_events'] += 1
                    event_stats['clean_events_count'] += 1
                    
                    # Create event data for UI
                    clean_event_data = {
                        'timestamp': datetime.now().strftime('%H:%M:%S'),
                        'key': key,
                        'value': event,
                        'topic': CLEAN_TOPIC,
                        'partition': message.partition,
                        'offset': message.offset
                    }
                    
                    recent_events['kafka_clean'].append(clean_event_data)
                    if len(recent_events['kafka_clean']) > 100:
                        recent_events['kafka_clean'].pop(0)
                    
                    recent_events['clean'].append(clean_event_data)
                    if len(recent_events['clean']) > 100:
                        recent_events['clean'].pop(0)
                    
                    # Emit to WebSocket
                    if socketio:
                        socketio.emit('kafka_clean_event', {
                            'type': 'clean',
                            'data': clean_event_data,
                            'stats': event_stats.copy()
                        })
                    
                    logger.info(f"Consumed clean event | key={key} | type={event.get('event_type')} | partition={message.partition} | offset={message.offset}")
            else:
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error in Kafka clean consumer thread: {e}")
            time.sleep(5)

# Routes for different dashboards
@app.route('/')
def main_dashboard():
    logger.info("Main dashboard accessed")
    return render_template('kafka_main_dashboard.html')

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
    return jsonify(event_stats)

@app.route('/api/kafka_stats')
def get_kafka_stats():
    return jsonify({
        'raw_events': event_stats['kafka_raw_events'],
        'clean_events': event_stats['kafka_clean_events'],
        'producer_active': not stop_flags.get('kafka_producer', False),
        'consumer_active': not stop_flags.get('kafka_consumer_raw', False),
        'topics': {
            'raw_topic': RAW_TOPIC,
            'clean_topic': CLEAN_TOPIC
        },
        'bootstrap_servers': BOOTSTRAP_SERVERS
    })

@app.route('/api/recent_events/<event_type>')
def get_recent_events(event_type):
    return jsonify(recent_events.get(event_type, []))

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
    emit('stats_update', event_stats)
    if storage_manager:
        emit('storage_update', {
            'storage_stats': storage_manager.get_combined_stats()
        })

@socketio.on('disconnect')
def handle_disconnect():
    logger.info('Client disconnected')

@socketio.on('start_kafka_producer')
def handle_start_kafka_producer():
    """Start Kafka producer"""
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
    global threads, stop_flags
    
    # Stop existing consumers
    for consumer_type in ['kafka_consumer_raw', 'kafka_consumer_clean']:
        if consumer_type in stop_flags:
            stop_flags[consumer_type] = True
    
    time.sleep(1)
    
    # Start raw consumer
    stop_flags['kafka_consumer_raw'] = False
    threads['kafka_consumer_raw'] = threading.Thread(target=kafka_consumer_raw_thread)
    threads['kafka_consumer_raw'].daemon = True
    threads['kafka_consumer_raw'].start()
    
    # Start clean consumer
    stop_flags['kafka_consumer_clean'] = False
    threads['kafka_consumer_clean'] = threading.Thread(target=kafka_consumer_clean_thread)
    threads['kafka_consumer_clean'].daemon = True
    threads['kafka_consumer_clean'].start()
    
    emit('kafka_consumers_started', {'status': 'Kafka consumers started'})

@socketio.on('stop_kafka_consumers')
def handle_stop_kafka_consumers():
    """Stop Kafka consumers"""
    global stop_flags
    
    for consumer_type in ['kafka_consumer_raw', 'kafka_consumer_clean']:
        if consumer_type in stop_flags:
            stop_flags[consumer_type] = True
    
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
        'last_raw_event': None,
        'last_clean_event': None,
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
    socketio.emit('stats_update', event_stats)

if __name__ == '__main__':
    logger.info("Starting Kafka-Enabled Multi-Dashboard E-commerce Analytics...")
    logger.info("Features: Kafka Integration, Multi-storage (Parquet + DuckDB), ETL Pipeline, Snowflake Integration")
    logger.info("Dashboards available:")
    logger.info("  Main: http://localhost:5004")
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