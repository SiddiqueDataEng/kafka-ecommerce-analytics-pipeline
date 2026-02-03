# 🏔️ Snowflake Setup Guide for E-commerce Analytics Pipeline

This guide provides comprehensive instructions for setting up the complete Snowflake infrastructure for the e-commerce analytics pipeline.

## 📋 Overview

The Snowflake setup creates a complete data warehouse infrastructure optimized for real-time e-commerce analytics:

### 🏗️ Infrastructure Components

**Warehouse:**
- `KAFKA_ANALYTICS_WH` - Medium-sized warehouse with auto-suspend/resume

**Database:**
- `ECOMMERCE_ANALYTICS` - Main database for all analytics data

**Schemas:**
- `RAW_DATA` - Raw streaming data from Kafka
- `PROCESSED_DATA` - Cleaned and structured data
- `ANALYTICS` - Views and aggregated data
- `STAGING` - Temporary processing area

## 🚀 Quick Setup

### Step 1: Configure Credentials
Your credentials are already configured in `snowflake_config.json`:
```json
{
  "user": "Siddique",
  "account": "AYFVEHQ-GL46164",
  "warehouse": "KAFKA_ANALYTICS_WH",
  "database": "ECOMMERCE_ANALYTICS"
}
```

### Step 2: Run Setup Script
```bash
setup_snowflake.bat
```

This will create the complete infrastructure automatically.

### Step 3: Verify Setup
The setup script will verify all components were created successfully.

## 📊 Database Schema

### RAW_DATA Schema

**RAW_EVENTS Table**
- Stores all incoming events from Kafka
- Includes data quality metadata
- VARIANT column for flexible JSON storage

**RAW_SESSIONS Table**
- Raw session data with device/browser info
- Traffic source and UTM tracking
- Geographic and demographic data

**RAW_CUSTOMERS Table**
- Customer profile information
- Demographic and behavioral data
- Lifetime value tracking

### PROCESSED_DATA Schema

**PAGE_VIEWS Table**
```sql
CREATE TABLE PAGE_VIEWS (
    event_id VARCHAR(50) PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    page_url VARCHAR(1000),
    page_title VARCHAR(200),
    time_on_page INTEGER,
    scroll_depth INTEGER,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    traffic_source VARCHAR(50),
    is_bounce BOOLEAN
);
```

**PRODUCT_INTERACTIONS Table**
```sql
CREATE TABLE PRODUCT_INTERACTIONS (
    event_id VARCHAR(50) PRIMARY KEY,
    event_type VARCHAR(30) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    product_category VARCHAR(50),
    product_brand VARCHAR(50),
    product_price DECIMAL(10,2),
    quantity INTEGER,
    variant VARCHAR(50),
    discount_applied DECIMAL(5,4)
);
```

**PURCHASES Table**
```sql
CREATE TABLE PURCHASES (
    event_id VARCHAR(50) PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    items VARIANT,
    subtotal DECIMAL(12,2),
    tax_amount DECIMAL(12,2),
    shipping_cost DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    currency VARCHAR(3),
    payment_method VARCHAR(30),
    shipping_method VARCHAR(30),
    coupon_code VARCHAR(50),
    is_guest_checkout BOOLEAN
);
```

**SEARCHES Table**
```sql
CREATE TABLE SEARCHES (
    event_id VARCHAR(50) PRIMARY KEY,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    search_query VARCHAR(200),
    search_results_count INTEGER,
    search_category VARCHAR(50),
    search_filters VARIANT,
    clicked_result_position INTEGER,
    device_type VARCHAR(20)
);
```

**USER_ENGAGEMENT Table**
```sql
CREATE TABLE USER_ENGAGEMENT (
    event_id VARCHAR(50) PRIMARY KEY,
    event_type VARCHAR(30) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    product_id VARCHAR(50),
    rating INTEGER,
    review_text VARCHAR(2000),
    is_verified_purchase BOOLEAN,
    shared_content VARCHAR(50),
    share_platform VARCHAR(30),
    email VARCHAR(100),
    subscription_type VARCHAR(30)
);
```

**SESSIONS Table (Processed)**
```sql
CREATE TABLE SESSIONS (
    session_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_seconds INTEGER,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    operating_system VARCHAR(30),
    traffic_source VARCHAR(50),
    landing_page VARCHAR(1000),
    exit_page VARCHAR(1000),
    page_views INTEGER DEFAULT 0,
    unique_pages INTEGER DEFAULT 0,
    bounce_rate DECIMAL(5,4),
    conversion_flag BOOLEAN DEFAULT FALSE,
    total_purchase_amount DECIMAL(12,2) DEFAULT 0
);
```

**CUSTOMERS Table (Processed)**
```sql
CREATE TABLE CUSTOMERS (
    customer_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(101),
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(10),
    country VARCHAR(2),
    state VARCHAR(2),
    city VARCHAR(50),
    registration_date DATE,
    customer_segment VARCHAR(20),
    lifetime_value DECIMAL(12,2),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    avg_order_value DECIMAL(12,2) DEFAULT 0,
    last_purchase_date DATE,
    days_since_last_purchase INTEGER,
    is_active BOOLEAN
);
```

### ANALYTICS Schema

**Key Analytics Views:**

1. **DAILY_EVENT_SUMMARY**
   - Daily aggregation of all event types
   - Unique customers and sessions per day
   - Event counts by type

2. **CUSTOMER_JOURNEY**
   - Complete customer journey tracking
   - Session sequences and conversion paths
   - Purchase attribution

3. **PRODUCT_PERFORMANCE**
   - Product views, cart additions, purchases
   - Conversion rates (view-to-cart, cart-to-purchase)
   - Revenue by product

4. **REALTIME_METRICS**
   - Today's key metrics
   - Live dashboard data
   - Current performance indicators

5. **DATA_QUALITY_SUMMARY**
   - Data quality scores by day
   - Invalid event tracking
   - Quality trend analysis

## 🔄 Data Processing

### Automated Processing
The setup includes a stored procedure `PROCESS_RAW_EVENTS()` that:
1. Extracts events from RAW_EVENTS table
2. Validates and cleans data
3. Inserts into appropriate processed tables
4. Handles data type conversions
5. Manages duplicate prevention

### Automated Tasks
- `PROCESS_EVENTS_TASK` runs every 5 minutes
- Automatically processes new raw events
- Maintains real-time data availability

## 📈 Sample Queries

### Daily Event Summary
```sql
SELECT * FROM ANALYTICS.DAILY_EVENT_SUMMARY 
WHERE event_date = CURRENT_DATE()
ORDER BY event_count DESC;
```

### Customer Journey Analysis
```sql
SELECT 
    customer_id,
    COUNT(*) as total_sessions,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) as converted_sessions,
    SUM(purchase_amount) as total_revenue
FROM ANALYTICS.CUSTOMER_JOURNEY
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;
```

### Product Performance
```sql
SELECT 
    product_name,
    views,
    cart_adds,
    purchases,
    revenue,
    view_to_cart_rate,
    cart_to_purchase_rate
FROM ANALYTICS.PRODUCT_PERFORMANCE
ORDER BY revenue DESC
LIMIT 10;
```

### Real-time Metrics
```sql
SELECT * FROM ANALYTICS.REALTIME_METRICS;
```

### Data Quality Monitoring
```sql
SELECT 
    date,
    total_events,
    valid_events,
    invalid_events,
    quality_score_pct
FROM ANALYTICS.DATA_QUALITY_SUMMARY
ORDER BY date DESC
LIMIT 7;
```

## 🔐 Security & Permissions

### Roles Created
- `KAFKA_ANALYTICS_ADMIN` - Full access to all objects
- `KAFKA_ANALYTICS_USER` - Read/write access to tables
- `KAFKA_ANALYTICS_READONLY` - Read-only access

### Permissions
- Warehouse usage rights
- Database and schema access
- Table and view permissions
- Stored procedure execution rights

## 📊 Data Loading

### From Enhanced App
Use the data loading script to load JSON files:
```bash
load_to_snowflake.bat
```

This will:
1. Load JSON files from `data/processed/` directory
2. Insert into RAW_EVENTS table
3. Process into structured tables
4. Archive processed files
5. Display loading statistics

### Manual Loading
```sql
-- Load from stage
COPY INTO RAW_EVENTS
FROM @RAW_DATA.KAFKA_JSON_STAGE
FILE_FORMAT = JSON_FORMAT;

-- Process raw events
CALL PROCESS_RAW_EVENTS();
```

## 🔧 Maintenance

### Regular Tasks
1. Monitor data quality scores
2. Review processing task performance
3. Archive old data as needed
4. Update customer segments
5. Refresh analytics views

### Performance Optimization
- Warehouse auto-suspend/resume configured
- Clustering keys on high-volume tables
- Materialized views for complex analytics
- Automated statistics collection

## 🚨 Troubleshooting

### Common Issues

**Connection Problems:**
- Verify account identifier format
- Check username/password
- Ensure role has necessary permissions

**Data Loading Issues:**
- Verify JSON file format
- Check for duplicate event IDs
- Review data quality issues

**Processing Errors:**
- Check task execution history
- Review stored procedure logs
- Validate data types

### Monitoring Queries
```sql
-- Check task status
SHOW TASKS LIKE 'PROCESS_EVENTS_TASK';

-- View task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'PROCESS_EVENTS_TASK'
ORDER BY SCHEDULED_TIME DESC;

-- Check data freshness
SELECT 
    MAX(ingestion_time) as latest_raw_event,
    MAX(created_at) as latest_processed_event
FROM RAW_DATA.RAW_EVENTS, PROCESSED_DATA.PAGE_VIEWS;
```

## 📞 Support

For issues with the Snowflake setup:
1. Check the setup logs for specific errors
2. Verify your Snowflake account permissions
3. Review the troubleshooting section above
4. Check Snowflake documentation for account-specific settings

---

**🎉 Your Snowflake data warehouse is ready for real-time e-commerce analytics!**