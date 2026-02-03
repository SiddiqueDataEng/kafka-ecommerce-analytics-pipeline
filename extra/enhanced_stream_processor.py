#!/usr/bin/env python3
"""
Enhanced Kafka Stream Processor for E-commerce Analytics
Processes raw events, validates them, and forwards clean events
"""

import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "clean_events"
GROUP_ID = "enhanced-stream-processor"

# Valid event types
VALID_EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "PURCHASE", "SEARCH", "REVIEW"]

# Initialize Kafka Consumer
try:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )
    logger.info(f"Kafka Consumer initialized - Topic: {INPUT_TOPIC}, Group: {GROUP_ID}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Consumer: {e}")
    exit(1)

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3
    )
    logger.info(f"Kafka Producer initialized - Target Topic: {OUTPUT_TOPIC}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

def validate_event(event):
    """
    Comprehensive event validation
    Returns (is_valid, validation_errors)
    """
    errors = []
    
    # Check required fields
    if not event.get("event_id"):
        errors.append("missing_event_id")
    
    if not event.get("customer_id"):
        errors.append("missing_customer_id")
    
    if not event.get("event_type"):
        errors.append("missing_event_type")
    elif event.get("event_type") not in VALID_EVENT_TYPES:
        errors.append("invalid_event_type")
    
    # Validate timestamp
    try:
        event_time = datetime.fromisoformat(event.get("event_timestamp", "").replace('Z', '+00:00'))
        now = datetime.utcnow()
        
        # Check if timestamp is too far in the future (more than 1 hour)
        if event_time > now + timedelta(hours=1):
            errors.append("future_timestamp")
        
        # Check if timestamp is too old (more than 30 days)
        if event_time < now - timedelta(days=30):
            errors.append("old_timestamp")
            
    except (ValueError, TypeError):
        errors.append("invalid_timestamp")
    
    # Validate amount for monetary events
    monetary_events = ["ADD_TO_CART", "PURCHASE"]
    if event.get("event_type") in monetary_events:
        amount = event.get("amount")
        if amount is None:
            errors.append("missing_amount")
        elif not isinstance(amount, (int, float)) or amount <= 0:
            errors.append("invalid_amount")
    
    # Validate currency for monetary events
    if event.get("event_type") in monetary_events:
        currency = event.get("currency")
        if not currency:
            errors.append("missing_currency")
        elif currency not in ["USD", "EUR", "GBP", "CAD", "AUD"]:
            errors.append("invalid_currency")
    
    # Event-specific validations
    event_type = event.get("event_type")
    event_data = event.get("event_data", {})
    
    if event_type == "PRODUCT_VIEW":
        if not event_data.get("product_id"):
            errors.append("missing_product_id")
        if not event_data.get("category"):
            errors.append("missing_category")
    
    elif event_type == "ADD_TO_CART":
        if not event_data.get("product_id"):
            errors.append("missing_product_id")
        quantity = event_data.get("quantity")
        if not quantity or not isinstance(quantity, int) or quantity <= 0:
            errors.append("invalid_quantity")
    
    elif event_type == "PURCHASE":
        if not event_data.get("order_id"):
            errors.append("missing_order_id")
        if not event_data.get("payment_method"):
            errors.append("missing_payment_method")
    
    elif event_type == "SEARCH":
        if not event_data.get("search_query"):
            errors.append("missing_search_query")
    
    elif event_type == "REVIEW":
        if not event_data.get("product_id"):
            errors.append("missing_product_id")
        rating = event_data.get("rating")
        if not rating or not isinstance(rating, int) or rating < 1 or rating > 5:
            errors.append("invalid_rating")
    
    return len(errors) == 0, errors

def enrich_event(event):
    """
    Enrich event with additional metadata
    """
    # Add processing metadata
    event["processed_timestamp"] = datetime.utcnow().isoformat()
    event["processor_version"] = "enhanced_stream_processor_v1.0"
    
    # Add derived fields
    event_type = event.get("event_type")
    
    # Categorize event
    if event_type in ["PAGE_VIEW", "SEARCH"]:
        event["event_category"] = "engagement"
    elif event_type in ["PRODUCT_VIEW", "ADD_TO_CART"]:
        event["event_category"] = "consideration"
    elif event_type == "PURCHASE":
        event["event_category"] = "conversion"
    elif event_type == "REVIEW":
        event["event_category"] = "retention"
    else:
        event["event_category"] = "other"
    
    # Add business value score
    if event_type == "PAGE_VIEW":
        event["business_value_score"] = 1
    elif event_type == "PRODUCT_VIEW":
        event["business_value_score"] = 3
    elif event_type == "SEARCH":
        event["business_value_score"] = 2
    elif event_type == "ADD_TO_CART":
        event["business_value_score"] = 7
    elif event_type == "PURCHASE":
        event["business_value_score"] = 10
    elif event_type == "REVIEW":
        event["business_value_score"] = 5
    else:
        event["business_value_score"] = 0
    
    # Add session context (simplified)
    customer_id = event.get("customer_id", "")
    if customer_id:
        # Simple session ID based on customer and hour
        hour = datetime.utcnow().strftime("%Y%m%d%H")
        event["session_id"] = f"{customer_id}_{hour}"
    
    return event

def main():
    """Main stream processor loop"""
    logger.info("Starting Enhanced Kafka Stream Processor...")
    logger.info(f"Input Topic: {INPUT_TOPIC}")
    logger.info(f"Output Topic: {OUTPUT_TOPIC}")
    logger.info(f"Consumer Group: {GROUP_ID}")
    
    processed_count = 0
    valid_count = 0
    invalid_count = 0
    
    try:
        logger.info("Waiting for messages...")
        
        for message in consumer:
            try:
                key = message.key
                event = message.value
                
                processed_count += 1
                
                # Validate event
                is_valid, validation_errors = validate_event(event)
                
                if is_valid:
                    # Enrich valid event
                    enriched_event = enrich_event(event.copy())
                    
                    # Send to clean events topic
                    producer.send(
                        topic=OUTPUT_TOPIC,
                        key=key,
                        value=enriched_event
                    )
                    
                    valid_count += 1
                    
                    logger.info(
                        f"PROCESSED #{processed_count} | "
                        f"key={key} | "
                        f"type={event.get('event_type')} | "
                        f"category={enriched_event.get('event_category')} | "
                        f"value_score={enriched_event.get('business_value_score')} | "
                        f"✅ VALID"
                    )
                else:
                    invalid_count += 1
                    
                    logger.warning(
                        f"PROCESSED #{processed_count} | "
                        f"key={key} | "
                        f"type={event.get('event_type')} | "
                        f"errors={validation_errors} | "
                        f"❌ INVALID"
                    )
                
                # Commit offset
                consumer.commit()
                
                # Log statistics every 25 events
                if processed_count % 25 == 0:
                    quality_score = (valid_count / processed_count) * 100
                    logger.info(
                        f"STATS: Processed={processed_count}, "
                        f"Valid={valid_count}, "
                        f"Invalid={invalid_count}, "
                        f"Quality={quality_score:.1f}%"
                    )
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Still commit to avoid reprocessing the same message
                consumer.commit()
                continue
                
    except KeyboardInterrupt:
        logger.info("Stream processor interrupted by user")
    except Exception as e:
        logger.error(f"Stream processor error: {e}")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        
        if processed_count > 0:
            final_quality = (valid_count / processed_count) * 100
            logger.info(
                f"Stream processor stopped. Final stats: "
                f"Processed={processed_count}, "
                f"Valid={valid_count}, "
                f"Invalid={invalid_count}, "
                f"Quality={final_quality:.1f}%"
            )

if __name__ == "__main__":
    main()