import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "raw_events"

# Initialize Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3
    )
    logger.info(f"Kafka Producer initialized - {BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "PURCHASE", "SEARCH", "REVIEW"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past = now - timedelta(days=6)

    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_event():
    is_invalid = random.random() < 0.15  # 15% invalid events

    customer_id = f"CUST_{random.randint(1, 100)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10, 500), 2)
    currency = "USD"

    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id",
            "event_type", 
            "amount",
            "currency"
        ])
    
    # Generate event-specific data
    event_data = {}
    if event_type == "PAGE_VIEW":
        event_data = {
            "page_url": f"/products/{random.randint(1, 1000)}",
            "referrer": random.choice(["google.com", "facebook.com", "direct"]),
            "session_duration": random.randint(30, 300)
        }
        amount = 0  # Page views don't have monetary value
    elif event_type == "PRODUCT_VIEW":
        event_data = {
            "product_id": f"PROD_{random.randint(1, 500)}",
            "category": random.choice(["electronics", "clothing", "books"]),
            "price": amount
        }
    elif event_type == "SEARCH":
        event_data = {
            "search_query": random.choice(["laptop", "shoes", "book", "phone"]),
            "results_count": random.randint(0, 100)
        }
        amount = 0  # Searches don't have monetary value
    elif event_type == "REVIEW":
        event_data = {
            "product_id": f"PROD_{random.randint(1, 500)}",
            "rating": random.randint(1, 5),
            "review_length": random.randint(50, 500)
        }
        amount = 0  # Reviews don't have monetary value
    
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
        "event_data": event_data,
        "is_valid": not is_invalid,
        "invalid_field": invalid_field,
        "source": "enhanced_producer"
    }

    return event["customer_id"], event

logger.info("Starting Enhanced Kafka Producer...")
logger.info(f"Target Topic: {TOPIC_NAME}")
logger.info(f"Bootstrap Servers: {BOOTSTRAP_SERVERS}")

event_count = 0
valid_count = 0

try:
    while True:
        key, event = generate_event()

        producer.send(
            topic=TOPIC_NAME,
            key=key,
            value=event
        )

        event_count += 1
        if event['is_valid']:
            valid_count += 1

        logger.info(
            f"Produced event #{event_count} | "
            f"key={key} | "
            f"type={event['event_type']} | "
            f"valid={event['is_valid']} | "
            f"amount=${event['amount']:.2f}"
        )

        # Variable delay based on event type
        if event['event_type'] == 'PAGE_VIEW':
            delay = random.uniform(0.5, 2.0)
        elif event['event_type'] == 'PURCHASE':
            delay = random.uniform(10.0, 30.0)
        else:
            delay = random.uniform(1.0, 5.0)

        time.sleep(delay)

except KeyboardInterrupt:
    logger.info("Producer interrupted by user")
except Exception as e:
    logger.error(f"Producer error: {e}")
finally:
    producer.flush()
    producer.close()
    quality_score = (valid_count / event_count) * 100 if event_count > 0 else 0
    logger.info(f"Producer stopped. Stats: Total={event_count}, Valid={valid_count}, Quality={quality_score:.1f}%")