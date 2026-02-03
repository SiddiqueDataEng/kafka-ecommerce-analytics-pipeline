import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import os
import random
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import uuid

logger = logging.getLogger(__name__)

class MultiStorageManager:
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.parquet_path = self.base_path / "parquet"
        self.duckdb_path = self.base_path / "duckdb"
        self.archive_path = self.base_path / "archive"
        
        for path in [self.parquet_path, self.duckdb_path, self.archive_path]:
            path.mkdir(exist_ok=True)
        
        # Initialize DuckDB
        self.duckdb_file = self.duckdb_path / "ecommerce_events.db"
        self.init_duckdb()
        
        # Batch settings
        self.batch_size = 500
        self.parquet_batch = []
        self.duckdb_batch = []
        
        # Statistics
        self.stats = {
            'parquet_events': 0,
            'duckdb_events': 0,
            'parquet_files': 0,
            'total_events': 0
        }
    
    def init_duckdb(self):
        """Initialize DuckDB database and tables"""
        try:
            conn = duckdb.connect(str(self.duckdb_file))
            
            # Create events table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id VARCHAR PRIMARY KEY,
                    event_type VARCHAR,
                    session_id VARCHAR,
                    customer_id VARCHAR,
                    timestamp TIMESTAMP,
                    event_data JSON,
                    data_quality_issues JSON,
                    is_valid BOOLEAN,
                    storage_type VARCHAR DEFAULT 'duckdb',
                    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create sessions table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id VARCHAR PRIMARY KEY,
                    customer_id VARCHAR,
                    start_time TIMESTAMP,
                    device_type VARCHAR,
                    browser VARCHAR,
                    operating_system VARCHAR,
                    traffic_source VARCHAR,
                    landing_page VARCHAR,
                    is_mobile BOOLEAN,
                    is_new_visitor BOOLEAN,
                    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create customers table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id VARCHAR PRIMARY KEY,
                    email VARCHAR,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    country VARCHAR,
                    city VARCHAR,
                    customer_segment VARCHAR,
                    lifetime_value DECIMAL(12,2),
                    registration_date DATE,
                    is_active BOOLEAN,
                    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.close()
            logger.info("DuckDB initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing DuckDB: {e}")
    
    def add_event(self, event):
        """Add event to either Parquet or DuckDB randomly"""
        try:
            # Randomly choose storage (50/50 split)
            use_parquet = random.choice([True, False])
            
            # Add storage metadata
            event['storage_type'] = 'parquet' if use_parquet else 'duckdb'
            event['ingestion_time'] = datetime.now().isoformat()
            
            if use_parquet:
                self.parquet_batch.append(event)
                self.stats['parquet_events'] += 1
                
                if len(self.parquet_batch) >= self.batch_size:
                    self.flush_parquet_batch()
            else:
                self.duckdb_batch.append(event)
                self.stats['duckdb_events'] += 1
                
                if len(self.duckdb_batch) >= self.batch_size:
                    self.flush_duckdb_batch()
            
            self.stats['total_events'] += 1
            
        except Exception as e:
            logger.error(f"Error adding event: {e}")
    
    def flush_parquet_batch(self):
        """Flush Parquet batch to file"""
        if not self.parquet_batch:
            return None
        
        try:
            # Convert to DataFrame
            df = pd.json_normalize(self.parquet_batch)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"events_{timestamp}_{len(df)}.parquet"
            filepath = self.parquet_path / filename
            
            # Write to Parquet
            df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            
            logger.info(f"Saved {len(df)} events to Parquet: {filename}")
            
            self.parquet_batch = []
            self.stats['parquet_files'] += 1
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error flushing Parquet batch: {e}")
            return None
    
    def flush_duckdb_batch(self):
        """Flush DuckDB batch to database"""
        if not self.duckdb_batch:
            return 0
        
        try:
            conn = duckdb.connect(str(self.duckdb_file))
            
            inserted_count = 0
            for event in self.duckdb_batch:
                try:
                    # Insert event
                    conn.execute("""
                        INSERT OR REPLACE INTO events (
                            event_id, event_type, session_id, customer_id, 
                            timestamp, event_data, data_quality_issues, 
                            is_valid, storage_type, ingestion_time
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event.get('event_id'),
                        event.get('event_type'),
                        event.get('session_id'),
                        event.get('customer_id'),
                        event.get('timestamp'),
                        json.dumps(event),
                        json.dumps(event.get('data_quality_issues', [])),
                        event.get('is_valid', True),
                        event.get('storage_type', 'duckdb'),
                        event.get('ingestion_time')
                    ))
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting event {event.get('event_id')}: {e}")
                    continue
            
            conn.close()
            
            logger.info(f"Saved {inserted_count} events to DuckDB")
            
            self.duckdb_batch = []
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error flushing DuckDB batch: {e}")
            return 0
    
    def flush_all_batches(self):
        """Flush all pending batches"""
        results = {}
        
        if self.parquet_batch:
            results['parquet_file'] = self.flush_parquet_batch()
        
        if self.duckdb_batch:
            results['duckdb_count'] = self.flush_duckdb_batch()
        
        return results
    
    def get_parquet_stats(self):
        """Get Parquet storage statistics"""
        try:
            files = list(self.parquet_path.glob("*.parquet"))
            total_size = sum(f.stat().st_size for f in files)
            
            # Get record count from files
            total_records = 0
            for file in files:
                try:
                    df = pd.read_parquet(file)
                    total_records += len(df)
                except:
                    continue
            
            return {
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_records': total_records,
                'latest_file': max(files, key=lambda x: x.stat().st_mtime).name if files else None
            }
            
        except Exception as e:
            logger.error(f"Error getting Parquet stats: {e}")
            return {'file_count': 0, 'total_size_mb': 0, 'total_records': 0, 'latest_file': None}
    
    def get_duckdb_stats(self):
        """Get DuckDB storage statistics"""
        try:
            conn = duckdb.connect(str(self.duckdb_file))
            
            # Get event count
            result = conn.execute("SELECT COUNT(*) FROM events").fetchone()
            event_count = result[0] if result else 0
            
            # Get session count
            result = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
            session_count = result[0] if result else 0
            
            # Get customer count
            result = conn.execute("SELECT COUNT(*) FROM customers").fetchone()
            customer_count = result[0] if result else 0
            
            # Get database file size
            db_size = self.duckdb_file.stat().st_size if self.duckdb_file.exists() else 0
            
            conn.close()
            
            return {
                'event_count': event_count,
                'session_count': session_count,
                'customer_count': customer_count,
                'db_size_mb': round(db_size / (1024 * 1024), 2),
                'latest_event': self.get_latest_duckdb_event()
            }
            
        except Exception as e:
            logger.error(f"Error getting DuckDB stats: {e}")
            return {'event_count': 0, 'session_count': 0, 'customer_count': 0, 'db_size_mb': 0, 'latest_event': None}
    
    def get_latest_duckdb_event(self):
        """Get latest event from DuckDB"""
        try:
            conn = duckdb.connect(str(self.duckdb_file))
            result = conn.execute("""
                SELECT event_id, event_type, timestamp 
                FROM events 
                ORDER BY ingestion_time DESC 
                LIMIT 1
            """).fetchone()
            conn.close()
            
            if result:
                return {
                    'event_id': result[0],
                    'event_type': result[1],
                    'timestamp': result[2]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting latest DuckDB event: {e}")
            return None
    
    def get_parquet_events(self, limit=100):
        """Get recent events from Parquet files"""
        try:
            files = sorted(
                self.parquet_path.glob("*.parquet"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            events = []
            for file in files[:5]:  # Check last 5 files
                try:
                    df = pd.read_parquet(file)
                    file_events = df.to_dict('records')
                    events.extend(file_events)
                    
                    if len(events) >= limit:
                        break
                except:
                    continue
            
            return events[:limit]
            
        except Exception as e:
            logger.error(f"Error getting Parquet events: {e}")
            return []
    
    def get_duckdb_events(self, limit=100):
        """Get recent events from DuckDB"""
        try:
            conn = duckdb.connect(str(self.duckdb_file))
            
            result = conn.execute(f"""
                SELECT event_id, event_type, session_id, customer_id, 
                       timestamp, event_data, is_valid, ingestion_time
                FROM events 
                ORDER BY ingestion_time DESC 
                LIMIT {limit}
            """).fetchall()
            
            conn.close()
            
            events = []
            for row in result:
                events.append({
                    'event_id': row[0],
                    'event_type': row[1],
                    'session_id': row[2],
                    'customer_id': row[3],
                    'timestamp': row[4],
                    'event_data': json.loads(row[5]) if row[5] else {},
                    'is_valid': row[6],
                    'ingestion_time': row[7]
                })
            
            return events
            
        except Exception as e:
            logger.error(f"Error getting DuckDB events: {e}")
            return []
    
    def get_combined_stats(self):
        """Get combined statistics from both storage types"""
        parquet_stats = self.get_parquet_stats()
        duckdb_stats = self.get_duckdb_stats()
        
        return {
            'parquet': parquet_stats,
            'duckdb': duckdb_stats,
            'total_events': parquet_stats['total_records'] + duckdb_stats['event_count'],
            'storage_distribution': {
                'parquet_percentage': round((parquet_stats['total_records'] / max(parquet_stats['total_records'] + duckdb_stats['event_count'], 1)) * 100, 1),
                'duckdb_percentage': round((duckdb_stats['event_count'] / max(parquet_stats['total_records'] + duckdb_stats['event_count'], 1)) * 100, 1)
            }
        }
    
    def cleanup_old_files(self, days_to_keep=7):
        """Clean up old Parquet files"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            deleted_files = []
            
            for file in self.parquet_path.glob("*.parquet"):
                if file.stat().st_mtime < cutoff_date.timestamp():
                    try:
                        # Move to archive instead of deleting
                        archive_file = self.archive_path / file.name
                        file.rename(archive_file)
                        deleted_files.append(str(file))
                        logger.info(f"Archived old file: {file}")
                    except Exception as e:
                        logger.error(f"Error archiving file {file}: {e}")
            
            return deleted_files
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return []