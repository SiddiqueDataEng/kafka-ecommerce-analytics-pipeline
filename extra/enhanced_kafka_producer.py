#!/usr/bin/env python3
"""
Enhanced Kafka Producer for E-commerce Analytics
Generates realistic e-commerce events and sends them to Kafka
"""

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

# Kafka Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "raw_events"

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3,
        batch_size=16384,
        linger_ms=10
    )
    logger.info(f"Kafka Producer initialized successfully - {BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

# Event Types and Configuration
EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "PURCHASE", "SEARCH", "REVIEW"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY", "BROWSE"]

# Product Categories
CATEGORIES = ["electronics", "clothing", "books", "home", "sports", "beauty", "toys"]

# Traffic Sources
TRAFFIC_SOURCES = ["google.com", "facebook.com", "instagram.com", "direct", "email", "youtube.com"]

# Payment Methods
PAYMENT_METHODS = ["credit_card", "paypal", "apple_pay", "google_pay", "bank_transfer"]

def random_timestamp_last_hours(hours=24):
    """Generate random timestamp within last N hours"""
    now = datetime.utcnow()
    past = now - timedelta(hours=hours)
    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_realistic_event():
    """Generate realistic e-commerce event"""
    is_invalid = random.random() < 0.15  # 15% invalid events
    
    # Basic event properties
    customer_id = f"CUST_{random.randint(1, 1000)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10, 500), 2)
    currency = "USD"
    
    # Determine invalid field if event is invalid
    invalid_field = None
    if is_invalid:
        invalid_field = random.choice([
            "customer_id", "event_type", "amount", "currency", "timestamp"
        ])
    
    # Generate event-specific data
    event_data = {}
    
    if event_type == "PAGE_VIEW":
        event_data = {
            "page_url": f"/products/{random.randint(1, 1000)}",
            "page_title": f"Product {random.randint(1, 1000)}",
            "referrer": random.choice(TRAFFIC_SOURCES),
            "session_duration": random.randint(30, 600),
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        }
        amount = 0  # Page views don't have monetary value
        
    elif event_type == "PRODUCT_VIEW":
        product_id = f"PROD_{random.randint(1, 500)}"
        category = random.choice(CATEGORIES)
        event_data = {
            "product_id": product_id,
            "product_name": f"{category.title()} Product {random.randint(1, 100)}",
            "category": category,
            "price": amount,
            "brand": f"Brand_{random.randint(1, 50)}",
            "in_stock": random.choice([True, False]),
            "view_duration": random.randint(10, 300)
        }
        
    elif event_type == "ADD_TO_CART":
        product_id = f"PROD_{random.randint(1, 500)}"
        quantity = random.randint(1, 5)
        event_data = {
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": amount,
            "total_price": amount * quantity,
            "category": random.choice(CATEGORIES),
            "cart_total_items": random.randint(1, 10),
            "cart_total_value": round(random.uniform(50, 1000), 2)
        }
        amount = amount * quantity
        
    elif event_type == "PURCHASE":
        order_id = f"ORDER_{random.randint(10000, 99999)}"
        items_count = random.randint(1, 5)
        event_data = {
            "order_id": order_id,
            "total_amount": amount,
            "items_count": items_count,
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_cost": round(random.uniform(5, 25), 2),
            "tax_amount": round(amount * 0.08, 2),
            "discount_amount": round(random.uniform(0, 50), 2),
            "shipping_address": {
                "country": random.choice(["US", "CA", "UK", "DE", "FR"]),
                "state": f"State_{random.randint(1, 50)}",
                "city": f"City_{random.randint(1, 100)}"
            }
        }
        
    elif event_type == "SEARCH":
        search_queries = ["laptop", "shoes", "book", "phone", "headphones", "watch", "camera", "tablet"]
        query = random.choice(search_queries)
        event_data = {
            "search_query": query,
            "results_count": random.randint(0, 500),
            "search_filters": {
                "category": random.choice(CATEGORIES),
                "price_min": random.randint(0, 100),
                "price_max": random.randint(100, 1000)
            },
            "search_duration": random.randint(5, 120),
            "clicked_result": random.choice([True, False])
        }
        amount = 0  # Searches don't have monetary value
        
    elif event_type == "REVIEW":
        product_id = f"PROD_{random.randint(1, 500)}"
        rating = random.randint(1, 5)
        event_data = {
            "product_id": product_id,
            "rating": rating,
            "review_text_length": random.randint(50, 500),
            "is_verified_purchase": random.choice([True, False]),
            "helpful_votes": random.randint(0, 50),
            "review_category": random.choice(["quality", "value", "shipping", "service"])
        }
        amount = 0  # Reviews don't have monetary value
    
    # Apply invalid field modifications
    if invalid_field == "customer_id":
        customer_id = None
    elif invalid_field == "event_type":
        event_type = random.choice(INVALID_EVENT_TYPES)
    elif invalid_field == "amount":
        amount = random.uniform(-500, -10)  # Negative amount
    elif invalid_field == "currency":
        currency = None
    elif invalid_field == "timestamp":
        # Future timestamp (invalid)
        event_timestamp = (datetime.utcnow() + timedelta(days=random.randint(1, 30))).isoformat()
    else:
        event_timestamp = random_timestamp_last_hours(6).isoformat()
    
    # Construct final event
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "event_type": event_type,
        "amount": amount,
        "currency": currency,
        "event_timestamp": event_timestamp if 'event_timestamp' in locals() else random_timestamp_last_hours(6).isoformat(),
        "event_data": event_data,
        "is_valid": not is_invalid,
        "invalid_field": invalid_field,
        "source": "enhanced_kafka_producer",
        "producer_timestamp": datetime.utcnow().isoformat()
    }
    
    return customer_id, event

def main():
    """Main producer loop"""
    logger.info("Starting Enhanced Kafka Producer...")
    logger.info(f"Target Topic: {TOPIC_NAME}")
    logger.info(f"Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    
    event_count = 0
    valid_count = 0
    invalid_count = 0
    
    try:
        while True:
            key, event = generate_realistic_event()
            
            # Send to Kafka
            future = producer.send(
                topic=TOPIC_NAME,
                key=key,
                value=event
            )
            
            # Update counters
            event_count += 1
            if event['is_valid']:
                valid_count += 1
            else:
                invalid_count += 1
            
            # Log event
            logger.info(
                f"Produced event #{event_count} | "
                f"key={key} | "
                f"type={event['event_type']} | "
                f"valid={event['is_valid']} | "
                f"amount=${event['amount']:.2f}"
            )
            
            # Log statistics every 50 events
            if event_count % 50 == 0:
                quality_score = (valid_count / event_count) * 100
                logger.info(
                    f"Statistics: Total={event_count}, "
                    f"Valid={valid_count}, "
                    f"Invalid={invalid_count}, "
                    f"Quality={quality_score:.1f}%"
                )
            
            # Variable delay based on event type
            if event['event_type'] == 'PAGE_VIEW':
                delay = random.uniform(0.5, 2.0)
            elif event['event_type'] == 'PURCHASE':
                delay = random.uniform(10.0, 30.0)
            elif event['event_type'] == 'SEARCH':
                delay = random.uniform(2.0, 8.0)
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
        logger.info(f"Producer stopped. Final stats: Total={event_count}, Valid={valid_count}, Invalid={invalid_count}")

if __name__ == "__main__":
    main()