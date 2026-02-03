#!/usr/bin/env python3
"""
ETL Pipeline for E-commerce Analytics
Extracts data from Parquet files and DuckDB, transforms and loads into Snowflake
with deduplication and data quality checks
"""

import pandas as pd
import duckdb
import snowflake.connector
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import hashlib
from multi_storage_manager import MultiStorageManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, snowflake_config_file='snowflake_config.json'):
        """Initialize ETL Pipeline"""
        self.snowflake_config = self.load_snowflake_config(snowflake_config_file)
        self.storage_manager = MultiStorageManager()
        self.snowflake_conn = None
        
        # ETL Statistics
        self.etl_stats = {
            'parquet_records_extracted': 0,
            'duckdb_records_extracted': 0,
            'total_records_extracted': 0,
            'duplicates_removed': 0,
            'records_loaded': 0,
            'data_quality_issues': 0,
            'processing_time_seconds': 0,
            'last_run': None
        }
    
    def load_snowflake_config(self, config_file):
        """Load Snowflake configuration"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            required_fields = ['user', 'password', 'account']
            for field in required_fields:
                if not config.get(field) or config[field] == f'YOUR_{field.upper()}':
                    raise ValueError(f"Please update {field} in {config_file}")
            
            return config
            
        except Exception as e:
            logger.error(f"Error loading Snowflake config: {e}")
            raise
    
    def connect_snowflake(self):
        """Connect to Snowflake"""
        try:
            self.snowflake_conn = snowflake.connector.connect(
                user=self.snowflake_config['user'],
                password=self.snowflake_config['password'],
                account=self.snowflake_config['account'],
                warehouse='KAFKA_ANALYTICS_WH',
                database='ECOMMERCE_ANALYTICS',
                schema='RAW_DATA'
            )
            logger.info("Connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def extract_parquet_data(self):
        """Extract data from Parquet files"""
        try:
            parquet_path = Path("data/parquet")
            parquet_files = list(parquet_path.glob("*.parquet"))
            
            if not parquet_files:
                logger.info("No Parquet files found")
                return pd.DataFrame()
            
            logger.info(f"Found {len(parquet_files)} Parquet files")
            
            # Read all Parquet files
            dfs = []
            for file in parquet_files:
                try:
                    df = pd.read_parquet(file)
                    df['source_file'] = file.name
                    df['extraction_time'] = datetime.now()
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading {file}: {e}")
                    continue
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                self.etl_stats['parquet_records_extracted'] = len(combined_df)
                logger.info(f"Extracted {len(combined_df)} records from Parquet files")
                return combined_df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error extracting Parquet data: {e}")
            return pd.DataFrame()
    
    def extract_duckdb_data(self):
        """Extract data from DuckDB"""
        try:
            duckdb_file = Path("data/duckdb/ecommerce_events.db")
            
            if not duckdb_file.exists():
                logger.info("DuckDB file not found")
                return pd.DataFrame()
            
            conn = duckdb.connect(str(duckdb_file))
            
            # Extract events
            query = """
                SELECT 
                    event_id, event_type, session_id, customer_id, 
                    timestamp, event_data, data_quality_issues, 
                    is_valid, storage_type, ingestion_time
                FROM events
                ORDER BY ingestion_time DESC
            """
            
            df = conn.execute(query).df()
            conn.close()
            
            if not df.empty:
                df['source_file'] = 'duckdb'
                df['extraction_time'] = datetime.now()
                self.etl_stats['duckdb_records_extracted'] = len(df)
                logger.info(f"Extracted {len(df)} records from DuckDB")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting DuckDB data: {e}")
            return pd.DataFrame()
    
    def transform_data(self, df):
        """Transform and clean the data"""
        if df.empty:
            return df
        
        try:
            logger.info("Starting data transformation...")
            
            # Remove duplicates based on event_id
            initial_count = len(df)
            df = df.drop_duplicates(subset=['event_id'], keep='first')
            duplicates_removed = initial_count - len(df)
            self.etl_stats['duplicates_removed'] = duplicates_removed
            
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate records")
            
            # Data quality checks
            quality_issues = 0
            
            # Check for missing required fields
            required_fields = ['event_id', 'event_type', 'timestamp']
            for field in required_fields:
                if field in df.columns:
                    missing_count = df[field].isna().sum()
                    if missing_count > 0:
                        logger.warning(f"Found {missing_count} records with missing {field}")
                        quality_issues += missing_count
            
            # Check for future timestamps
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                future_timestamps = df['timestamp'] > datetime.now()
                future_count = future_timestamps.sum()
                if future_count > 0:
                    logger.warning(f"Found {future_count} records with future timestamps")
                    quality_issues += future_count
            
            # Standardize data types
            if 'is_valid' in df.columns:
                df['is_valid'] = df['is_valid'].fillna(True)
            
            # Parse JSON fields if they're strings
            json_fields = ['event_data', 'data_quality_issues']
            for field in json_fields:
                if field in df.columns:
                    df[field] = df[field].apply(lambda x: 
                        json.loads(x) if isinstance(x, str) else x
                    )
            
            # Add ETL metadata
            df['etl_run_id'] = str(datetime.now().strftime('%Y%m%d_%H%M%S'))
            df['etl_timestamp'] = datetime.now()
            
            self.etl_stats['data_quality_issues'] = quality_issues
            logger.info(f"Transformation completed. {quality_issues} quality issues found")
            
            return df
            
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            return df
    
    def load_to_snowflake(self, df):
        """Load transformed data to Snowflake"""
        if df.empty:
            logger.info("No data to load")
            return True
        
        try:
            cursor = self.snowflake_conn.cursor()
            
            logger.info(f"Loading {len(df)} records to Snowflake...")
            
            # Insert records into RAW_EVENTS table
            inserted_count = 0
            for _, row in df.iterrows():
                try:
                    # Prepare data for insertion
                    event_data = {
                        'event_id': row.get('event_id'),
                        'event_type': row.get('event_type'),
                        'session_id': row.get('session_id'),
                        'customer_id': row.get('customer_id'),
                        'timestamp': row.get('timestamp'),
                        'event_data': json.dumps(row.get('event_data', {})),
                        'data_quality_issues': json.dumps(row.get('data_quality_issues', [])),
                        'is_valid': row.get('is_valid', True),
                        'source_file': row.get('source_file'),
                        'storage_type': row.get('storage_type', 'unknown'),
                        'etl_run_id': row.get('etl_run_id'),
                        'etl_timestamp': row.get('etl_timestamp')
                    }
                    
                    # Insert with ON CONFLICT handling (upsert)
                    insert_sql = """
                    MERGE INTO RAW_EVENTS AS target
                    USING (
                        SELECT 
                            %(event_id)s as event_id,
                            %(event_type)s as event_type,
                            %(session_id)s as session_id,
                            %(customer_id)s as customer_id,
                            %(timestamp)s as timestamp,
                            PARSE_JSON(%(event_data)s) as event_data,
                            PARSE_JSON(%(data_quality_issues)s) as data_quality_issues,
                            %(is_valid)s as is_valid,
                            %(source_file)s as source_file,
                            %(storage_type)s as storage_type,
                            %(etl_run_id)s as etl_run_id,
                            %(etl_timestamp)s as etl_timestamp
                    ) AS source
                    ON target.event_id = source.event_id
                    WHEN NOT MATCHED THEN
                        INSERT (
                            event_id, event_type, session_id, customer_id, 
                            timestamp, event_data, data_quality_issues, 
                            is_valid, source_file, storage_type, 
                            etl_run_id, etl_timestamp
                        )
                        VALUES (
                            source.event_id, source.event_type, source.session_id, 
                            source.customer_id, source.timestamp, source.event_data, 
                            source.data_quality_issues, source.is_valid, 
                            source.source_file, source.storage_type,
                            source.etl_run_id, source.etl_timestamp
                        )
                    WHEN MATCHED THEN
                        UPDATE SET
                            event_type = source.event_type,
                            session_id = source.session_id,
                            customer_id = source.customer_id,
                            timestamp = source.timestamp,
                            event_data = source.event_data,
                            data_quality_issues = source.data_quality_issues,
                            is_valid = source.is_valid,
                            etl_timestamp = source.etl_timestamp
                    """
                    
                    cursor.execute(insert_sql, event_data)
                    inserted_count += 1
                    
                except Exception as e:
                    logger.error(f"Error inserting record {row.get('event_id', 'unknown')}: {e}")
                    continue
            
            # Process raw events into structured tables
            logger.info("Processing raw events into structured tables...")
            cursor.execute("CALL PROCESS_RAW_EVENTS()")
            result = cursor.fetchone()
            logger.info(f"Processing result: {result[0]}")
            
            cursor.close()
            
            self.etl_stats['records_loaded'] = inserted_count
            logger.info(f"Successfully loaded {inserted_count} records to Snowflake")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {e}")
            return False
    
    def archive_processed_files(self):
        """Archive processed Parquet files"""
        try:
            parquet_path = Path("data/parquet")
            archive_path = Path("data/archive")
            archive_path.mkdir(exist_ok=True)
            
            archived_files = []
            for file in parquet_path.glob("*.parquet"):
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archive_file = archive_path / f"{file.stem}_{timestamp}.parquet"
                    file.rename(archive_file)
                    archived_files.append(str(file))
                    logger.info(f"Archived {file.name}")
                except Exception as e:
                    logger.error(f"Error archiving {file}: {e}")
            
            return archived_files
            
        except Exception as e:
            logger.error(f"Error during archiving: {e}")
            return []
    
    def run_etl(self, archive_files=True):
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        logger.info("Starting ETL pipeline...")
        
        try:
            # Connect to Snowflake
            if not self.connect_snowflake():
                return False
            
            # Extract data from both sources
            logger.info("Extracting data from sources...")
            parquet_df = self.extract_parquet_data()
            duckdb_df = self.extract_duckdb_data()
            
            # Combine data
            if not parquet_df.empty and not duckdb_df.empty:
                combined_df = pd.concat([parquet_df, duckdb_df], ignore_index=True)
            elif not parquet_df.empty:
                combined_df = parquet_df
            elif not duckdb_df.empty:
                combined_df = duckdb_df
            else:
                logger.info("No data found in either source")
                return True
            
            self.etl_stats['total_records_extracted'] = len(combined_df)
            logger.info(f"Total records extracted: {len(combined_df)}")
            
            # Transform data
            transformed_df = self.transform_data(combined_df)
            
            # Load to Snowflake
            success = self.load_to_snowflake(transformed_df)
            
            if success and archive_files:
                # Archive processed files
                self.archive_processed_files()
            
            # Update statistics
            end_time = datetime.now()
            self.etl_stats['processing_time_seconds'] = (end_time - start_time).total_seconds()
            self.etl_stats['last_run'] = end_time.isoformat()
            
            logger.info("ETL pipeline completed successfully")
            logger.info(f"Processing time: {self.etl_stats['processing_time_seconds']:.2f} seconds")
            
            return success
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return False
        finally:
            if self.snowflake_conn:
                self.snowflake_conn.close()
    
    def get_etl_stats(self):
        """Get ETL pipeline statistics"""
        return self.etl_stats.copy()
    
    def get_snowflake_summary(self):
        """Get summary statistics from Snowflake"""
        try:
            if not self.connect_snowflake():
                return None
            
            cursor = self.snowflake_conn.cursor()
            
            # Get raw events count
            cursor.execute("SELECT COUNT(*) FROM RAW_EVENTS")
            raw_count = cursor.fetchone()[0]
            
            # Get processed events by type
            cursor.execute("USE SCHEMA PROCESSED_DATA")
            
            stats_queries = {
                'page_views': "SELECT COUNT(*) FROM PAGE_VIEWS",
                'product_interactions': "SELECT COUNT(*) FROM PRODUCT_INTERACTIONS",
                'purchases': "SELECT COUNT(*) FROM PURCHASES",
                'searches': "SELECT COUNT(*) FROM SEARCHES",
                'user_engagement': "SELECT COUNT(*) FROM USER_ENGAGEMENT",
                'sessions': "SELECT COUNT(*) FROM SESSIONS",
                'customers': "SELECT COUNT(*) FROM CUSTOMERS"
            }
            
            processed_stats = {}
            for key, query in stats_queries.items():
                try:
                    cursor.execute(query)
                    processed_stats[key] = cursor.fetchone()[0]
                except:
                    processed_stats[key] = 0
            
            # Get data quality metrics
            cursor.execute("USE SCHEMA ANALYTICS")
            cursor.execute("""
                SELECT 
                    total_events, valid_events, invalid_events, quality_score_pct
                FROM DATA_QUALITY_SUMMARY 
                WHERE date = CURRENT_DATE()
                LIMIT 1
            """)
            
            quality_result = cursor.fetchone()
            quality_stats = {}
            if quality_result:
                quality_stats = {
                    'total_events': quality_result[0],
                    'valid_events': quality_result[1],
                    'invalid_events': quality_result[2],
                    'quality_score': quality_result[3]
                }
            
            cursor.close()
            self.snowflake_conn.close()
            
            return {
                'raw_events': raw_count,
                'processed_events': processed_stats,
                'data_quality': quality_stats,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting Snowflake summary: {e}")
            return None

def main():
    """Main execution function"""
    logger.info("Starting ETL Pipeline...")
    
    etl = ETLPipeline()
    
    try:
        success = etl.run_etl()
        
        if success:
            stats = etl.get_etl_stats()
            logger.info("ETL Statistics:")
            logger.info(f"  Parquet records: {stats['parquet_records_extracted']}")
            logger.info(f"  DuckDB records: {stats['duckdb_records_extracted']}")
            logger.info(f"  Total extracted: {stats['total_records_extracted']}")
            logger.info(f"  Duplicates removed: {stats['duplicates_removed']}")
            logger.info(f"  Records loaded: {stats['records_loaded']}")
            logger.info(f"  Processing time: {stats['processing_time_seconds']:.2f}s")
        else:
            logger.error("ETL pipeline failed")
    
    except KeyboardInterrupt:
        logger.info("ETL interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()