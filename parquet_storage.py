import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class ParquetStorageManager:
    def __init__(self, base_path="data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)
        
        # Create subdirectories for different data types
        self.raw_path = self.base_path / "raw"
        self.processed_path = self.base_path / "processed"
        self.failed_path = self.base_path / "failed"
        
        for path in [self.raw_path, self.processed_path, self.failed_path]:
            path.mkdir(exist_ok=True)
        
        # Batch settings
        self.batch_size = 1000
        self.current_batches = {
            'raw': [],
            'processed': [],
            'failed': []
        }
    
    def add_event(self, event, event_category='raw'):
        """Add an event to the current batch"""
        self.current_batches[event_category].append(event)
        
        # Check if batch is full
        if len(self.current_batches[event_category]) >= self.batch_size:
            self.flush_batch(event_category)
    
    def flush_batch(self, event_category='raw'):
        """Flush current batch to parquet file"""
        if not self.current_batches[event_category]:
            return None
        
        try:
            # Convert events to DataFrame
            df = pd.json_normalize(self.current_batches[event_category])
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{event_category}_events_{timestamp}_{len(df)}.parquet"
            
            # Determine path based on category
            if event_category == 'raw':
                filepath = self.raw_path / filename
            elif event_category == 'processed':
                filepath = self.processed_path / filename
            else:
                filepath = self.failed_path / filename
            
            # Write to parquet
            df.to_parquet(filepath, engine='pyarrow', compression='snappy')
            
            logger.info(f"Saved {len(df)} events to {filepath}")
            
            # Clear the batch
            self.current_batches[event_category] = []
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving batch to parquet: {e}")
            return None
    
    def flush_all_batches(self):
        """Flush all current batches"""
        filepaths = []
        for category in self.current_batches.keys():
            filepath = self.flush_batch(category)
            if filepath:
                filepaths.append(filepath)
        return filepaths
    
    def get_parquet_files(self, category='processed', days_back=7):
        """Get list of parquet files for a category within date range"""
        if category == 'raw':
            search_path = self.raw_path
        elif category == 'processed':
            search_path = self.processed_path
        else:
            search_path = self.failed_path
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        files = []
        for file in search_path.glob("*.parquet"):
            if file.stat().st_mtime > cutoff_date.timestamp():
                files.append(file)
        
        return sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)
    
    def read_parquet_files(self, category='processed', days_back=7):
        """Read and combine parquet files into a single DataFrame"""
        files = self.get_parquet_files(category, days_back)
        
        if not files:
            return pd.DataFrame()
        
        try:
            # Read all files and combine
            dfs = []
            for file in files:
                df = pd.read_parquet(file)
                dfs.append(df)
            
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Combined {len(files)} files into DataFrame with {len(combined_df)} rows")
            
            return combined_df
            
        except Exception as e:
            logger.error(f"Error reading parquet files: {e}")
            return pd.DataFrame()
    
    def get_storage_stats(self):
        """Get storage statistics"""
        stats = {}
        
        for category, path in [('raw', self.raw_path), ('processed', self.processed_path), ('failed', self.failed_path)]:
            files = list(path.glob("*.parquet"))
            total_size = sum(f.stat().st_size for f in files)
            
            stats[category] = {
                'file_count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'latest_file': max(files, key=lambda x: x.stat().st_mtime).name if files else None
            }
        
        return stats
    
    def cleanup_old_files(self, days_to_keep=30):
        """Clean up old parquet files"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        deleted_files = []
        
        for path in [self.raw_path, self.processed_path, self.failed_path]:
            for file in path.glob("*.parquet"):
                if file.stat().st_mtime < cutoff_date.timestamp():
                    try:
                        file.unlink()
                        deleted_files.append(str(file))
                        logger.info(f"Deleted old file: {file}")
                    except Exception as e:
                        logger.error(f"Error deleting file {file}: {e}")
        
        return deleted_files

class SnowflakeParquetUploader:
    def __init__(self, snowflake_config):
        self.config = snowflake_config
        self.stage_name = "KAFKA_PARQUET_STAGE"
        self.table_mappings = {
            'page_views': 'PAGE_VIEWS',
            'product_interactions': 'PRODUCT_INTERACTIONS', 
            'purchases': 'PURCHASES',
            'searches': 'SEARCHES',
            'user_engagement': 'USER_ENGAGEMENT',
            'sessions': 'SESSIONS',
            'customers': 'CUSTOMERS'
        }
    
    def setup_snowflake_objects(self):
        """Create necessary Snowflake objects (stages, tables, etc.)"""
        try:
            import snowflake.connector
            
            conn = snowflake.connector.connect(**self.config)
            cursor = conn.cursor()
            
            # Create stage for parquet files
            cursor.execute(f"""
                CREATE STAGE IF NOT EXISTS {self.stage_name}
                FILE_FORMAT = (TYPE = PARQUET)
                COMMENT = 'Stage for Kafka parquet files'
            """)
            
            # Create tables for different event types
            self._create_tables(cursor)
            
            cursor.close()
            conn.close()
            
            logger.info("Snowflake objects created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Snowflake objects: {e}")
            return False
    
    def _create_tables(self, cursor):
        """Create Snowflake tables for different event types"""
        
        # Page Views table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PAGE_VIEWS (
                event_id VARCHAR(50) PRIMARY KEY,
                event_type VARCHAR(20),
                session_id VARCHAR(50),
                customer_id VARCHAR(50),
                timestamp TIMESTAMP_NTZ,
                page_url VARCHAR(500),
                page_title VARCHAR(200),
                previous_page VARCHAR(500),
                time_on_page INTEGER,
                scroll_depth INTEGER,
                device_type VARCHAR(20),
                browser VARCHAR(50),
                traffic_source VARCHAR(50),
                is_bounce BOOLEAN,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Product Interactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PRODUCT_INTERACTIONS (
                event_id VARCHAR(50) PRIMARY KEY,
                event_type VARCHAR(30),
                session_id VARCHAR(50),
                customer_id VARCHAR(50),
                timestamp TIMESTAMP_NTZ,
                product_id VARCHAR(50),
                product_name VARCHAR(200),
                product_category VARCHAR(50),
                product_brand VARCHAR(50),
                product_price DECIMAL(10,2),
                quantity INTEGER,
                variant VARCHAR(50),
                discount_applied DECIMAL(5,4),
                device_type VARCHAR(20),
                page_url VARCHAR(500),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Purchases table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PURCHASES (
                event_id VARCHAR(50) PRIMARY KEY,
                event_type VARCHAR(20),
                session_id VARCHAR(50),
                customer_id VARCHAR(50),
                timestamp TIMESTAMP_NTZ,
                order_id VARCHAR(50),
                items VARIANT,
                subtotal DECIMAL(10,2),
                tax_amount DECIMAL(10,2),
                shipping_cost DECIMAL(10,2),
                discount_amount DECIMAL(10,2),
                total_amount DECIMAL(10,2),
                currency VARCHAR(3),
                payment_method VARCHAR(30),
                shipping_method VARCHAR(30),
                coupon_code VARCHAR(50),
                device_type VARCHAR(20),
                is_guest_checkout BOOLEAN,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Searches table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SEARCHES (
                event_id VARCHAR(50) PRIMARY KEY,
                event_type VARCHAR(20),
                session_id VARCHAR(50),
                customer_id VARCHAR(50),
                timestamp TIMESTAMP_NTZ,
                search_query VARCHAR(200),
                search_results_count INTEGER,
                search_category VARCHAR(50),
                search_filters VARIANT,
                clicked_result_position INTEGER,
                device_type VARCHAR(20),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # User Engagement table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS USER_ENGAGEMENT (
                event_id VARCHAR(50) PRIMARY KEY,
                event_type VARCHAR(30),
                session_id VARCHAR(50),
                customer_id VARCHAR(50),
                timestamp TIMESTAMP_NTZ,
                device_type VARCHAR(20),
                product_id VARCHAR(50),
                rating INTEGER,
                review_text VARCHAR(1000),
                is_verified_purchase BOOLEAN,
                shared_content VARCHAR(50),
                share_platform VARCHAR(30),
                shared_url VARCHAR(500),
                email VARCHAR(100),
                subscription_type VARCHAR(30),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Sessions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS SESSIONS (
                session_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50),
                start_time TIMESTAMP_NTZ,
                device_type VARCHAR(20),
                browser VARCHAR(50),
                operating_system VARCHAR(30),
                screen_resolution VARCHAR(20),
                user_agent VARCHAR(500),
                ip_address VARCHAR(45),
                traffic_source VARCHAR(50),
                utm_campaign VARCHAR(100),
                utm_medium VARCHAR(50),
                utm_source VARCHAR(50),
                referrer_url VARCHAR(500),
                landing_page VARCHAR(500),
                is_mobile BOOLEAN,
                is_new_visitor BOOLEAN,
                page_views INTEGER,
                session_duration INTEGER,
                bounce_rate DECIMAL(5,4),
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # Customers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CUSTOMERS (
                customer_id VARCHAR(50) PRIMARY KEY,
                email VARCHAR(100),
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                phone VARCHAR(20),
                date_of_birth DATE,
                gender VARCHAR(10),
                country VARCHAR(2),
                state VARCHAR(2),
                city VARCHAR(50),
                postal_code VARCHAR(20),
                registration_date DATE,
                customer_segment VARCHAR(20),
                lifetime_value DECIMAL(10,2),
                is_active BOOLEAN,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
    
    def upload_parquet_to_snowflake(self, parquet_file_path, table_name=None):
        """Upload a parquet file to Snowflake"""
        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas
            
            # Read parquet file
            df = pd.read_parquet(parquet_file_path)
            
            if df.empty:
                logger.warning(f"Parquet file {parquet_file_path} is empty")
                return False
            
            # Determine table name based on event types in the data
            if not table_name:
                table_name = self._determine_table_name(df)
            
            # Connect to Snowflake
            conn = snowflake.connector.connect(**self.config)
            
            # Prepare DataFrame for Snowflake
            df_prepared = self._prepare_dataframe_for_snowflake(df, table_name)
            
            # Upload to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df_prepared,
                table_name=table_name,
                auto_create_table=False,
                overwrite=False
            )
            
            conn.close()
            
            if success:
                logger.info(f"Successfully uploaded {nrows} rows to {table_name}")
                return True
            else:
                logger.error(f"Failed to upload to {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading parquet to Snowflake: {e}")
            return False
    
    def _determine_table_name(self, df):
        """Determine appropriate table name based on event types"""
        if 'event_type' not in df.columns:
            return 'MIXED_EVENTS'
        
        event_types = df['event_type'].unique()
        
        if 'PAGE_VIEW' in event_types:
            return 'PAGE_VIEWS'
        elif any(et in ['PRODUCT_VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'ADD_TO_WISHLIST'] for et in event_types):
            return 'PRODUCT_INTERACTIONS'
        elif 'PURCHASE' in event_types:
            return 'PURCHASES'
        elif 'SEARCH' in event_types:
            return 'SEARCHES'
        elif any(et in ['REVIEW', 'RATING', 'SHARE', 'NEWSLETTER_SIGNUP'] for et in event_types):
            return 'USER_ENGAGEMENT'
        else:
            return 'MIXED_EVENTS'
    
    def _prepare_dataframe_for_snowflake(self, df, table_name):
        """Prepare DataFrame for Snowflake upload"""
        df_copy = df.copy()
        
        # Convert timestamp columns
        timestamp_columns = ['timestamp', 'start_time', 'created_at']
        for col in timestamp_columns:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col])
        
        # Handle JSON columns (convert to string for VARIANT type)
        json_columns = ['items', 'search_filters', 'data_quality_issues']
        for col in json_columns:
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].apply(lambda x: json.dumps(x) if pd.notna(x) else None)
        
        # Ensure column names are uppercase (Snowflake convention)
        df_copy.columns = [col.upper() for col in df_copy.columns]
        
        return df_copy
    
    def batch_upload_parquet_files(self, parquet_files):
        """Upload multiple parquet files to Snowflake"""
        results = []
        
        for file_path in parquet_files:
            try:
                success = self.upload_parquet_to_snowflake(file_path)
                results.append({
                    'file': str(file_path),
                    'success': success,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"Error uploading {file_path}: {e}")
                results.append({
                    'file': str(file_path),
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
        
        return results