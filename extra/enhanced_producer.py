import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from data_generators import EcommerceDataGenerator
from parquet_storage import ParquetStorageManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9092"
RAW_TOPIC = "raw_events"

class EnhancedKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        
        self.data_generator = EcommerceDataGenerator()
        self.storage_manager = ParquetStorageManager()
        self.session_pool = {}
        self.stats = {
            'events_produced': 0,
            'events_stored': 0,
            'sessions_created': 0,
            'parquet_files_created': 0
        }
    
    def produce_realistic_events(self, duration_minutes=60, events_per_minute=100):
        """Produce realistic e-commerce events for specified duration"""
        logger.info(f"Starting enhanced producer for {duration_minutes} minutes")
        logger.info(f"Target rate: {events_per_minute} events per minute")
        
        start_time = datetime.now()
        end_time = start_time.timestamp() + (duration_minutes * 60)
        
        event_interval = 60.0 / events_per_minute  # seconds between events
        
        try:
            while datetime.now().timestamp() < end_time:
                # Generate realistic event
                event = self._generate_realistic_event()
                
                # Send to Kafka
                key = event.get('customer_id')
                future = self.producer.send(RAW_TOPIC, key=key, value=event)
                
                # Store in Parquet
                self.storage_manager.add_event(event, 'raw')
                
                # Update stats
                self.stats['events_produced'] += 1
                self.stats['events_stored'] += 1
                
                # Log progress
                if self.stats['events_produced'] % 100 == 0:
                    logger.info(f"Produced {self.stats['events_produced']} events")
                
                # Flush parquet batches periodically
                if self.stats['events_produced'] % 1000 == 0:
                    files = self.storage_manager.flush_all_batches()
                    self.stats['parquet_files_created'] += len(files)
                    logger.info(f"Created {len(files)} parquet files")
                
                # Wait for next event
                time.sleep(event_interval)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            # Flush remaining events
            self.producer.flush()
            files = self.storage_manager.flush_all_batches()
            self.stats['parquet_files_created'] += len(files)
            
            logger.info("Producer shutdown complete")
            logger.info(f"Final stats: {self.stats}")
    
    def _generate_realistic_event(self):
        """Generate a realistic event with proper session management"""
        # Decide whether to use existing session or create new one
        if self.session_pool and len(self.session_pool) > 0 and random.random() < 0.7:
            session_id = random.choice(list(self.session_pool.keys()))
        else:
            # Create new session
            session = self.data_generator.generate_session_data()
            session_id = session['session_id']
            self.session_pool[session_id] = session
            self.stats['sessions_created'] += 1
            
            # Limit session pool size
            if len(self.session_pool) > 100:
                oldest_session = min(self.session_pool.keys())
                del self.session_pool[oldest_session]
        
        # Generate event
        event = self.data_generator.generate_random_event(session_id)
        
        # Add data quality issues
        event, is_valid = self.data_generator.add_data_quality_issues(event)
        
        return event
    
    def produce_specific_event_type(self, event_type, count=100):
        """Produce specific type of events for testing"""
        logger.info(f"Producing {count} {event_type} events")
        
        generators = {
            'PAGE_VIEW': self.data_generator.generate_page_view_event,
            'PRODUCT_INTERACTION': self.data_generator.generate_product_interaction_event,
            'PURCHASE': self.data_generator.generate_purchase_event,
            'SEARCH': self.data_generator.generate_search_event,
            'USER_ENGAGEMENT': self.data_generator.generate_user_engagement_event
        }
        
        generator = generators.get(event_type)
        if not generator:
            logger.error(f"Unknown event type: {event_type}")
            return
        
        try:
            for i in range(count):
                event = generator()
                event, is_valid = self.data_generator.add_data_quality_issues(event)
                
                key = event.get('customer_id')
                self.producer.send(RAW_TOPIC, key=key, value=event)
                self.storage_manager.add_event(event, 'raw')
                
                self.stats['events_produced'] += 1
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Produced {i + 1}/{count} {event_type} events")
                
                time.sleep(0.1)  # Small delay
                
        except Exception as e:
            logger.error(f"Error producing {event_type} events: {e}")
        finally:
            self.producer.flush()
            files = self.storage_manager.flush_all_batches()
            logger.info(f"Created {len(files)} parquet files")

if __name__ == "__main__":
    import random
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Kafka Producer')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes')
    parser.add_argument('--rate', type=int, default=100, help='Events per minute')
    parser.add_argument('--event-type', type=str, help='Specific event type to produce')
    parser.add_argument('--count', type=int, default=100, help='Number of specific events')
    
    args = parser.parse_args()
    
    producer = EnhancedKafkaProducer()
    
    if args.event_type:
        producer.produce_specific_event_type(args.event_type, args.count)
    else:
        producer.produce_realistic_events(args.duration, args.rate)