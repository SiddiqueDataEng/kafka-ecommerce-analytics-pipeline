#!/usr/bin/env python3
"""
Snowflake Setup Script for E-commerce Analytics Pipeline
This script creates the complete Snowflake infrastructure including:
- Warehouse, Database, and Schemas
- Tables for raw and processed data
- Analytics views and stored procedures
- Data quality monitoring
- Automated tasks
"""

import snowflake.connector
import json
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowflakeSetup:
    def __init__(self, config_file='snowflake_config.json'):
        """Initialize with Snowflake configuration"""
        self.config = self.load_config(config_file)
        self.connection = None
        
    def load_config(self, config_file):
        """Load Snowflake configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            # Validate required fields
            required_fields = ['user', 'password', 'account']
            for field in required_fields:
                if not config.get(field) or config[field] == f'YOUR_{field.upper()}':
                    raise ValueError(f"Please update {field} in {config_file}")
            
            return config
            
        except FileNotFoundError:
            logger.error(f"Configuration file {config_file} not found")
            sys.exit(1)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in {config_file}")
            sys.exit(1)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)
    
    def connect(self):
        """Connect to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config.get('warehouse', 'COMPUTE_WH'),
                database=self.config.get('database', 'KAFKA_DB'),
                schema=self.config.get('schema', 'PUBLIC'),
                role=self.config.get('role', 'ACCOUNTADMIN')
            )
            logger.info("Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def execute_sql_file(self, sql_file):
        """Execute SQL commands from file"""
        try:
            with open(sql_file, 'r') as f:
                sql_content = f.read()
            
            # Split SQL content by semicolons and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            cursor = self.connection.cursor()
            
            for i, statement in enumerate(statements, 1):
                try:
                    # Skip comments and empty statements
                    if statement.startswith('--') or not statement:
                        continue
                    
                    logger.info(f"Executing statement {i}/{len(statements)}")
                    cursor.execute(statement)
                    
                    # Log results for certain statements
                    if any(keyword in statement.upper() for keyword in ['CREATE', 'ALTER', 'GRANT']):
                        logger.info(f"✓ Executed: {statement[:50]}...")
                    
                except Exception as e:
                    logger.error(f"Error executing statement {i}: {e}")
                    logger.error(f"Statement: {statement[:100]}...")
                    continue
            
            cursor.close()
            logger.info("SQL file execution completed")
            return True
            
        except FileNotFoundError:
            logger.error(f"SQL file {sql_file} not found")
            return False
        except Exception as e:
            logger.error(f"Error executing SQL file: {e}")
            return False
    
    def verify_setup(self):
        """Verify that the setup was successful"""
        try:
            cursor = self.connection.cursor()
            
            # Check warehouse
            cursor.execute("SHOW WAREHOUSES LIKE 'KAFKA_ANALYTICS_WH'")
            warehouses = cursor.fetchall()
            if warehouses:
                logger.info("✓ Warehouse KAFKA_ANALYTICS_WH created successfully")
            else:
                logger.warning("⚠ Warehouse KAFKA_ANALYTICS_WH not found")
            
            # Check database
            cursor.execute("SHOW DATABASES LIKE 'ECOMMERCE_ANALYTICS'")
            databases = cursor.fetchall()
            if databases:
                logger.info("✓ Database ECOMMERCE_ANALYTICS created successfully")
            else:
                logger.warning("⚠ Database ECOMMERCE_ANALYTICS not found")
            
            # Check schemas
            cursor.execute("USE DATABASE ECOMMERCE_ANALYTICS")
            cursor.execute("SHOW SCHEMAS")
            schemas = [row[1] for row in cursor.fetchall()]
            
            expected_schemas = ['RAW_DATA', 'PROCESSED_DATA', 'ANALYTICS', 'STAGING']
            for schema in expected_schemas:
                if schema in schemas:
                    logger.info(f"✓ Schema {schema} created successfully")
                else:
                    logger.warning(f"⚠ Schema {schema} not found")
            
            # Check some key tables
            cursor.execute("USE SCHEMA RAW_DATA")
            cursor.execute("SHOW TABLES")
            raw_tables = [row[1] for row in cursor.fetchall()]
            
            expected_raw_tables = ['RAW_EVENTS', 'RAW_SESSIONS', 'RAW_CUSTOMERS']
            for table in expected_raw_tables:
                if table in raw_tables:
                    logger.info(f"✓ Table RAW_DATA.{table} created successfully")
                else:
                    logger.warning(f"⚠ Table RAW_DATA.{table} not found")
            
            # Check processed tables
            cursor.execute("USE SCHEMA PROCESSED_DATA")
            cursor.execute("SHOW TABLES")
            processed_tables = [row[1] for row in cursor.fetchall()]
            
            expected_processed_tables = ['PAGE_VIEWS', 'PRODUCT_INTERACTIONS', 'PURCHASES', 'SEARCHES', 'USER_ENGAGEMENT', 'SESSIONS', 'CUSTOMERS']
            for table in expected_processed_tables:
                if table in processed_tables:
                    logger.info(f"✓ Table PROCESSED_DATA.{table} created successfully")
                else:
                    logger.warning(f"⚠ Table PROCESSED_DATA.{table} not found")
            
            # Check views
            cursor.execute("USE SCHEMA ANALYTICS")
            cursor.execute("SHOW VIEWS")
            views = [row[1] for row in cursor.fetchall()]
            
            expected_views = ['DAILY_EVENT_SUMMARY', 'CUSTOMER_JOURNEY', 'PRODUCT_PERFORMANCE', 'REALTIME_METRICS', 'DATA_QUALITY_SUMMARY']
            for view in expected_views:
                if view in views:
                    logger.info(f"✓ View ANALYTICS.{view} created successfully")
                else:
                    logger.warning(f"⚠ View ANALYTICS.{view} not found")
            
            cursor.close()
            logger.info("Setup verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            return False
    
    def create_sample_data(self):
        """Insert sample data for testing"""
        try:
            cursor = self.connection.cursor()
            
            # Insert sample raw event
            sample_event_sql = """
            INSERT INTO RAW_DATA.RAW_EVENTS (
                event_id, event_type, session_id, customer_id, timestamp, 
                event_data, is_valid
            ) VALUES (
                'TEST_EVENT_001',
                'PAGE_VIEW',
                'TEST_SESSION_001',
                'TEST_CUSTOMER_001',
                CURRENT_TIMESTAMP(),
                PARSE_JSON('{"page_url": "/test", "device_type": "Desktop", "browser": "Chrome"}'),
                TRUE
            )
            """
            
            cursor.execute(sample_event_sql)
            logger.info("✓ Sample data inserted successfully")
            
            # Test the processing procedure
            cursor.execute("CALL PROCESS_RAW_EVENTS()")
            result = cursor.fetchone()
            logger.info(f"✓ Processing procedure executed: {result[0]}")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"Error creating sample data: {e}")
            return False
    
    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            logger.info("Connection closed")

def main():
    """Main execution function"""
    logger.info("Starting Snowflake setup for E-commerce Analytics Pipeline")
    
    # Initialize setup
    setup = SnowflakeSetup()
    
    try:
        # Connect to Snowflake
        if not setup.connect():
            sys.exit(1)
        
        # Execute setup SQL
        logger.info("Executing Snowflake setup SQL...")
        if not setup.execute_sql_file('snowflake_setup.sql'):
            logger.error("Failed to execute setup SQL")
            sys.exit(1)
        
        # Verify setup
        logger.info("Verifying setup...")
        if not setup.verify_setup():
            logger.warning("Setup verification had issues")
        
        # Create sample data
        logger.info("Creating sample data...")
        if not setup.create_sample_data():
            logger.warning("Failed to create sample data")
        
        logger.info("🎉 Snowflake setup completed successfully!")
        logger.info("You can now use the following connection details:")
        logger.info(f"  Warehouse: KAFKA_ANALYTICS_WH")
        logger.info(f"  Database: ECOMMERCE_ANALYTICS")
        logger.info(f"  Schemas: RAW_DATA, PROCESSED_DATA, ANALYTICS, STAGING")
        
    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        setup.close()

if __name__ == "__main__":
    main()