import json
import logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "raw_events"
OUTPUT_TOPIC = "clean_events"
GROUP_ID = "enhanced-stream-processor"

VALID_EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "PURCHASE", "SEARCH", "REVIEW"]

try:
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    logger.info(f"Kafka Consumer initialized - Topic: {INPUT_TOPIC}, Group: {GROUP_ID}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Consumer: {e}")
    exit(1)

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

def is_valid_event(event):
    """Enhanced event validation"""
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    
    # Check monetary events
    if event.get("event_type") in ["ADD_TO_CART", "PURCHASE"]:
        if event.get("amount") is None or event.get("amount") <= 0:
            return False
        if not event.get("currency"):
            return False
    
    # Check timestamp
    try:
        event_time = datetime.fromisoformat(event.get("event_timestamp", "").replace('Z', '+00:00'))
        now = datetime.utcnow()
        if event_time > now + timedelta(hours=1):  # Future timestamp
            return False
    except (ValueError, TypeError):
        return False
    
    if event.get("is_valid") is not True:
        return False
    return True

def enrich_event(event):
    """Enrich event with additional metadata"""
    event["processed_timestamp"] = datetime.utcnow().isoformat()
    event["processor_version"] = "enhanced_stream_processor_v1.0"
    
    # Add event category
    event_type = event.get("event_type")
    if event_type in ["PAGE_VIEW", "SEARCH"]:
        event["event_category"] = "engagement"
    elif event_type in ["PRODUCT_VIEW", "ADD_TO_CART"]:
        event["event_category"] = "consideration"
    elif event_type == "PURCHASE":
        event["event_category"] = "conversion"
    elif event_type == "REVIEW":
        event["event_category"] = "retention"
    
    # Add business value score
    value_scores = {
        "PAGE_VIEW": 1,
        "PRODUCT_VIEW": 3,
        "SEARCH": 2,
        "ADD_TO_CART": 7,
        "PURCHASE": 10,
        "REVIEW": 5
    }
    event["business_value_score"] = value_scores.get(event_type, 0)
    
    return event

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
            
            if is_valid_event(event):
                # Enrich and forward valid event
                enriched_event = enrich_event(event.copy())
                
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
                    f"❌ INVALID"
                )
            
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