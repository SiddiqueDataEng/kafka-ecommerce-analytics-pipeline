-- ========================================================================
-- Snowflake Setup for E-commerce Analytics Pipeline
-- ========================================================================

-- Create Warehouse
CREATE OR REPLACE WAREHOUSE KAFKA_ANALYTICS_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for Kafka e-commerce analytics pipeline';

-- Use the warehouse
USE WAREHOUSE KAFKA_ANALYTICS_WH;

-- Create Database
CREATE OR REPLACE DATABASE ECOMMERCE_ANALYTICS
    COMMENT = 'Database for e-commerce streaming analytics data';

-- Use the database
USE DATABASE ECOMMERCE_ANALYTICS;

-- Create Schemas
CREATE OR REPLACE SCHEMA RAW_DATA
    COMMENT = 'Schema for raw streaming data from Kafka';

CREATE OR REPLACE SCHEMA PROCESSED_DATA
    COMMENT = 'Schema for cleaned and processed data';

CREATE OR REPLACE SCHEMA ANALYTICS
    COMMENT = 'Schema for analytics views and aggregated data';

CREATE OR REPLACE SCHEMA STAGING
    COMMENT = 'Schema for staging and temporary data processing';

-- Create File Formats
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'JSON file format for streaming data';

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'AUTO'
    COMMENT = 'Parquet file format for batch data';

-- Create Stages
CREATE OR REPLACE STAGE RAW_DATA.KAFKA_JSON_STAGE
    FILE_FORMAT = JSON_FORMAT
    COMMENT = 'Stage for JSON files from Kafka pipeline';

CREATE OR REPLACE STAGE RAW_DATA.KAFKA_PARQUET_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'Stage for Parquet files from Kafka pipeline';

-- ========================================================================
-- RAW DATA TABLES
-- ========================================================================

USE SCHEMA RAW_DATA;

-- Raw Events Table (All events from Kafka)
CREATE OR REPLACE TABLE RAW_EVENTS (
    event_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    session_id VARCHAR(50),
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    event_data VARIANT,
    data_quality_issues VARIANT,
    is_valid BOOLEAN DEFAULT TRUE,
    partition_id INTEGER,
    offset_id BIGINT,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(500),
    CONSTRAINT pk_raw_events PRIMARY KEY (event_id)
);

-- Raw Sessions Table
CREATE OR REPLACE TABLE RAW_SESSIONS (
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    start_time TIMESTAMP_NTZ,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    operating_system VARCHAR(30),
    screen_resolution VARCHAR(20),
    user_agent VARCHAR(1000),
    ip_address VARCHAR(45),
    traffic_source VARCHAR(50),
    utm_campaign VARCHAR(100),
    utm_medium VARCHAR(50),
    utm_source VARCHAR(50),
    referrer_url VARCHAR(1000),
    landing_page VARCHAR(1000),
    is_mobile BOOLEAN,
    is_new_visitor BOOLEAN,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_sessions PRIMARY KEY (session_id)
);

-- Raw Customers Table
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id VARCHAR(50) NOT NULL,
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
    lifetime_value DECIMAL(12,2),
    is_active BOOLEAN,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_raw_customers PRIMARY KEY (customer_id)
);

-- ========================================================================
-- PROCESSED DATA TABLES
-- ========================================================================

USE SCHEMA PROCESSED_DATA;

-- Page Views Table
CREATE OR REPLACE TABLE PAGE_VIEWS (
    event_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    page_url VARCHAR(1000),
    page_title VARCHAR(200),
    previous_page VARCHAR(1000),
    time_on_page INTEGER,
    scroll_depth INTEGER,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    traffic_source VARCHAR(50),
    is_bounce BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_page_views PRIMARY KEY (event_id)
);

-- Product Interactions Table
CREATE OR REPLACE TABLE PRODUCT_INTERACTIONS (
    event_id VARCHAR(50) NOT NULL,
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
    discount_applied DECIMAL(5,4),
    device_type VARCHAR(20),
    page_url VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_product_interactions PRIMARY KEY (event_id)
);

-- Purchases Table
CREATE OR REPLACE TABLE PURCHASES (
    event_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    order_id VARCHAR(50) NOT NULL,
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
    device_type VARCHAR(20),
    is_guest_checkout BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_purchases PRIMARY KEY (event_id),
    CONSTRAINT uk_purchases_order UNIQUE (order_id)
);

-- Purchase Items Table (Normalized from purchases)
CREATE OR REPLACE TABLE PURCHASE_ITEMS (
    purchase_item_id VARCHAR(50) NOT NULL,
    event_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(50),
    brand VARCHAR(50),
    price DECIMAL(10,2),
    quantity INTEGER,
    line_total DECIMAL(12,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_purchase_items PRIMARY KEY (purchase_item_id)
);

-- Searches Table
CREATE OR REPLACE TABLE SEARCHES (
    event_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    search_query VARCHAR(200),
    search_results_count INTEGER,
    search_category VARCHAR(50),
    search_filters VARIANT,
    clicked_result_position INTEGER,
    device_type VARCHAR(20),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_searches PRIMARY KEY (event_id)
);

-- User Engagement Table
CREATE OR REPLACE TABLE USER_ENGAGEMENT (
    event_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    timestamp TIMESTAMP_NTZ NOT NULL,
    device_type VARCHAR(20),
    product_id VARCHAR(50),
    rating INTEGER,
    review_text VARCHAR(2000),
    is_verified_purchase BOOLEAN,
    shared_content VARCHAR(50),
    share_platform VARCHAR(30),
    shared_url VARCHAR(1000),
    email VARCHAR(100),
    subscription_type VARCHAR(30),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_user_engagement PRIMARY KEY (event_id)
);

-- Sessions Table (Processed)
CREATE OR REPLACE TABLE SESSIONS (
    session_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    duration_seconds INTEGER,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    operating_system VARCHAR(30),
    screen_resolution VARCHAR(20),
    ip_address VARCHAR(45),
    traffic_source VARCHAR(50),
    utm_campaign VARCHAR(100),
    utm_medium VARCHAR(50),
    utm_source VARCHAR(50),
    referrer_url VARCHAR(1000),
    landing_page VARCHAR(1000),
    exit_page VARCHAR(1000),
    is_mobile BOOLEAN,
    is_new_visitor BOOLEAN,
    page_views INTEGER DEFAULT 0,
    unique_pages INTEGER DEFAULT 0,
    bounce_rate DECIMAL(5,4),
    conversion_flag BOOLEAN DEFAULT FALSE,
    total_purchase_amount DECIMAL(12,2) DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_sessions PRIMARY KEY (session_id)
);

-- Customers Table (Processed)
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(101),
    phone VARCHAR(20),
    date_of_birth DATE,
    age INTEGER,
    gender VARCHAR(10),
    country VARCHAR(2),
    state VARCHAR(2),
    city VARCHAR(50),
    postal_code VARCHAR(20),
    registration_date DATE,
    customer_segment VARCHAR(20),
    lifetime_value DECIMAL(12,2),
    total_orders INTEGER DEFAULT 0,
    total_spent DECIMAL(12,2) DEFAULT 0,
    avg_order_value DECIMAL(12,2) DEFAULT 0,
    last_purchase_date DATE,
    days_since_last_purchase INTEGER,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

-- ========================================================================
-- ANALYTICS VIEWS AND TABLES
-- ========================================================================

USE SCHEMA ANALYTICS;

-- Daily Event Summary
CREATE OR REPLACE VIEW DAILY_EVENT_SUMMARY AS
SELECT 
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions
FROM PROCESSED_DATA.PAGE_VIEWS
GROUP BY DATE(timestamp), event_type
UNION ALL
SELECT 
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions
FROM PROCESSED_DATA.PRODUCT_INTERACTIONS
GROUP BY DATE(timestamp), event_type
UNION ALL
SELECT 
    DATE(timestamp) as event_date,
    'PURCHASE' as event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions
FROM PROCESSED_DATA.PURCHASES
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as event_date,
    'SEARCH' as event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions
FROM PROCESSED_DATA.SEARCHES
GROUP BY DATE(timestamp)
UNION ALL
SELECT 
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT session_id) as unique_sessions
FROM PROCESSED_DATA.USER_ENGAGEMENT
GROUP BY DATE(timestamp), event_type;

-- Customer Journey Analysis
CREATE OR REPLACE VIEW CUSTOMER_JOURNEY AS
SELECT 
    s.customer_id,
    s.session_id,
    s.start_time,
    s.duration_seconds,
    s.page_views,
    s.device_type,
    s.traffic_source,
    CASE WHEN p.order_id IS NOT NULL THEN TRUE ELSE FALSE END as converted,
    p.total_amount as purchase_amount,
    ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.start_time) as session_sequence
FROM PROCESSED_DATA.SESSIONS s
LEFT JOIN PROCESSED_DATA.PURCHASES p ON s.session_id = p.session_id;

-- Product Performance
CREATE OR REPLACE VIEW PRODUCT_PERFORMANCE AS
SELECT 
    pi.product_id,
    pi.product_name,
    pi.product_category,
    pi.product_brand,
    COUNT(CASE WHEN pi.event_type = 'PRODUCT_VIEW' THEN 1 END) as views,
    COUNT(CASE WHEN pi.event_type = 'ADD_TO_CART' THEN 1 END) as cart_adds,
    COUNT(DISTINCT pit.order_id) as purchases,
    SUM(pit.quantity) as units_sold,
    SUM(pit.line_total) as revenue,
    ROUND(COUNT(CASE WHEN pi.event_type = 'ADD_TO_CART' THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN pi.event_type = 'PRODUCT_VIEW' THEN 1 END), 0) * 100, 2) as view_to_cart_rate,
    ROUND(COUNT(DISTINCT pit.order_id) / 
          NULLIF(COUNT(CASE WHEN pi.event_type = 'ADD_TO_CART' THEN 1 END), 0) * 100, 2) as cart_to_purchase_rate
FROM PROCESSED_DATA.PRODUCT_INTERACTIONS pi
LEFT JOIN PROCESSED_DATA.PURCHASE_ITEMS pit ON pi.product_id = pit.product_id
GROUP BY pi.product_id, pi.product_name, pi.product_category, pi.product_brand;

-- Real-time Dashboard Metrics
CREATE OR REPLACE VIEW REALTIME_METRICS AS
SELECT 
    COUNT(*) as total_events_today,
    COUNT(DISTINCT customer_id) as unique_customers_today,
    COUNT(DISTINCT session_id) as unique_sessions_today,
    SUM(CASE WHEN event_type = 'PAGE_VIEW' THEN 1 ELSE 0 END) as page_views_today,
    SUM(CASE WHEN event_type IN ('PRODUCT_VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART') THEN 1 ELSE 0 END) as product_interactions_today,
    (SELECT COUNT(*) FROM PROCESSED_DATA.PURCHASES WHERE DATE(timestamp) = CURRENT_DATE()) as purchases_today,
    (SELECT COUNT(*) FROM PROCESSED_DATA.SEARCHES WHERE DATE(timestamp) = CURRENT_DATE()) as searches_today,
    (SELECT SUM(total_amount) FROM PROCESSED_DATA.PURCHASES WHERE DATE(timestamp) = CURRENT_DATE()) as revenue_today
FROM (
    SELECT 'PAGE_VIEW' as event_type, customer_id, session_id FROM PROCESSED_DATA.PAGE_VIEWS WHERE DATE(timestamp) = CURRENT_DATE()
    UNION ALL
    SELECT event_type, customer_id, session_id FROM PROCESSED_DATA.PRODUCT_INTERACTIONS WHERE DATE(timestamp) = CURRENT_DATE()
    UNION ALL
    SELECT 'PURCHASE' as event_type, customer_id, session_id FROM PROCESSED_DATA.PURCHASES WHERE DATE(timestamp) = CURRENT_DATE()
    UNION ALL
    SELECT 'SEARCH' as event_type, customer_id, session_id FROM PROCESSED_DATA.SEARCHES WHERE DATE(timestamp) = CURRENT_DATE()
    UNION ALL
    SELECT event_type, customer_id, session_id FROM PROCESSED_DATA.USER_ENGAGEMENT WHERE DATE(timestamp) = CURRENT_DATE()
);

-- ========================================================================
-- DATA QUALITY MONITORING
-- ========================================================================

-- Data Quality Summary
CREATE OR REPLACE VIEW DATA_QUALITY_SUMMARY AS
SELECT 
    DATE(ingestion_time) as date,
    COUNT(*) as total_events,
    COUNT(CASE WHEN is_valid = TRUE THEN 1 END) as valid_events,
    COUNT(CASE WHEN is_valid = FALSE THEN 1 END) as invalid_events,
    ROUND(COUNT(CASE WHEN is_valid = TRUE THEN 1 END) / COUNT(*) * 100, 2) as quality_score_pct,
    COUNT(CASE WHEN data_quality_issues IS NOT NULL THEN 1 END) as events_with_issues
FROM RAW_DATA.RAW_EVENTS
GROUP BY DATE(ingestion_time)
ORDER BY date DESC;

-- ========================================================================
-- STORED PROCEDURES
-- ========================================================================

-- Procedure to process raw events into structured tables
CREATE OR REPLACE PROCEDURE PROCESS_RAW_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Process Page Views
    INSERT INTO PROCESSED_DATA.PAGE_VIEWS (
        event_id, session_id, customer_id, timestamp, page_url, page_title,
        previous_page, time_on_page, scroll_depth, device_type, browser,
        traffic_source, is_bounce
    )
    SELECT 
        event_id,
        session_id,
        customer_id,
        timestamp,
        event_data:page_url::VARCHAR,
        event_data:page_title::VARCHAR,
        event_data:previous_page::VARCHAR,
        event_data:time_on_page::INTEGER,
        event_data:scroll_depth::INTEGER,
        event_data:device_type::VARCHAR,
        event_data:browser::VARCHAR,
        event_data:traffic_source::VARCHAR,
        event_data:is_bounce::BOOLEAN
    FROM RAW_DATA.RAW_EVENTS
    WHERE event_type = 'PAGE_VIEW'
    AND is_valid = TRUE
    AND event_id NOT IN (SELECT event_id FROM PROCESSED_DATA.PAGE_VIEWS);
    
    -- Process Product Interactions
    INSERT INTO PROCESSED_DATA.PRODUCT_INTERACTIONS (
        event_id, event_type, session_id, customer_id, timestamp,
        product_id, product_name, product_category, product_brand,
        product_price, quantity, variant, discount_applied, device_type, page_url
    )
    SELECT 
        event_id,
        event_type,
        session_id,
        customer_id,
        timestamp,
        event_data:product_id::VARCHAR,
        event_data:product_name::VARCHAR,
        event_data:product_category::VARCHAR,
        event_data:product_brand::VARCHAR,
        event_data:product_price::DECIMAL(10,2),
        event_data:quantity::INTEGER,
        event_data:variant::VARCHAR,
        event_data:discount_applied::DECIMAL(5,4),
        event_data:device_type::VARCHAR,
        event_data:page_url::VARCHAR
    FROM RAW_DATA.RAW_EVENTS
    WHERE event_type IN ('PRODUCT_VIEW', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'ADD_TO_WISHLIST')
    AND is_valid = TRUE
    AND event_id NOT IN (SELECT event_id FROM PROCESSED_DATA.PRODUCT_INTERACTIONS);
    
    -- Process Purchases
    INSERT INTO PROCESSED_DATA.PURCHASES (
        event_id, session_id, customer_id, timestamp, order_id,
        items, subtotal, tax_amount, shipping_cost, discount_amount,
        total_amount, currency, payment_method, shipping_method,
        coupon_code, device_type, is_guest_checkout
    )
    SELECT 
        event_id,
        session_id,
        customer_id,
        timestamp,
        event_data:order_id::VARCHAR,
        event_data:items,
        event_data:subtotal::DECIMAL(12,2),
        event_data:tax_amount::DECIMAL(12,2),
        event_data:shipping_cost::DECIMAL(10,2),
        event_data:discount_amount::DECIMAL(10,2),
        event_data:total_amount::DECIMAL(12,2),
        event_data:currency::VARCHAR,
        event_data:payment_method::VARCHAR,
        event_data:shipping_method::VARCHAR,
        event_data:coupon_code::VARCHAR,
        event_data:device_type::VARCHAR,
        event_data:is_guest_checkout::BOOLEAN
    FROM RAW_DATA.RAW_EVENTS
    WHERE event_type = 'PURCHASE'
    AND is_valid = TRUE
    AND event_id NOT IN (SELECT event_id FROM PROCESSED_DATA.PURCHASES);
    
    RETURN 'Raw events processed successfully';
END;
$$;

-- ========================================================================
-- TASKS FOR AUTOMATION
-- ========================================================================

-- Task to process raw events every 5 minutes
CREATE OR REPLACE TASK PROCESS_EVENTS_TASK
    WAREHOUSE = KAFKA_ANALYTICS_WH
    SCHEDULE = '5 MINUTE'
AS
    CALL PROCESS_RAW_EVENTS();

-- Start the task
ALTER TASK PROCESS_EVENTS_TASK RESUME;

-- ========================================================================
-- GRANTS AND PERMISSIONS
-- ========================================================================

-- Create roles
CREATE OR REPLACE ROLE KAFKA_ANALYTICS_ADMIN;
CREATE OR REPLACE ROLE KAFKA_ANALYTICS_USER;
CREATE OR REPLACE ROLE KAFKA_ANALYTICS_READONLY;

-- Grant permissions to admin role
GRANT ALL ON WAREHOUSE KAFKA_ANALYTICS_WH TO ROLE KAFKA_ANALYTICS_ADMIN;
GRANT ALL ON DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_ADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_ADMIN;
GRANT ALL ON ALL TABLES IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_ADMIN;
GRANT ALL ON ALL VIEWS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_ADMIN;

-- Grant permissions to user role
GRANT USAGE ON WAREHOUSE KAFKA_ANALYTICS_WH TO ROLE KAFKA_ANALYTICS_USER;
GRANT USAGE ON DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_USER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_USER;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_USER;
GRANT SELECT ON ALL VIEWS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_USER;

-- Grant permissions to readonly role
GRANT USAGE ON WAREHOUSE KAFKA_ANALYTICS_WH TO ROLE KAFKA_ANALYTICS_READONLY;
GRANT USAGE ON DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_READONLY;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_READONLY;
GRANT SELECT ON ALL TABLES IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_READONLY;
GRANT SELECT ON ALL VIEWS IN DATABASE ECOMMERCE_ANALYTICS TO ROLE KAFKA_ANALYTICS_READONLY;

-- ========================================================================
-- SAMPLE QUERIES FOR TESTING
-- ========================================================================

-- Test data quality
SELECT * FROM ANALYTICS.DATA_QUALITY_SUMMARY ORDER BY date DESC LIMIT 7;

-- Test real-time metrics
SELECT * FROM ANALYTICS.REALTIME_METRICS;

-- Test customer journey
SELECT * FROM ANALYTICS.CUSTOMER_JOURNEY WHERE customer_id = 'CUST_12345' ORDER BY session_sequence;

-- Test product performance
SELECT * FROM ANALYTICS.PRODUCT_PERFORMANCE ORDER BY revenue DESC LIMIT 10;

-- Test daily summary
SELECT * FROM ANALYTICS.DAILY_EVENT_SUMMARY WHERE event_date = CURRENT_DATE();

COMMIT;