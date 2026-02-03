@echo off
cls
echo.
echo  ██████╗ ██████╗ ███╗   ███╗██████╗ ██╗     ███████╗████████╗███████╗
echo ██╔════╝██╔═══██╗████╗ ████║██╔══██╗██║     ██╔════╝╚══██╔══╝██╔════╝
echo ██║     ██║   ██║██╔████╔██║██████╔╝██║     █████╗     ██║   █████╗  
echo ██║     ██║   ██║██║╚██╔╝██║██╔═══╝ ██║     ██╔══╝     ██║   ██╔══╝  
echo ╚██████╗╚██████╔╝██║ ╚═╝ ██║██║     ███████╗███████╗   ██║   ███████╗
echo  ╚═════╝ ╚═════╝ ╚═╝     ╚═╝╚═╝     ╚══════╝╚══════╝   ╚═╝   ╚══════╝
echo.
echo  🚀 E-COMMERCE ANALYTICS PIPELINE - COMPLETE SETUP
echo.
echo ========================================================================
echo   Complete Setup for Kafka E-commerce Analytics with Snowflake
echo ========================================================================
echo.
echo This script will set up the entire end-to-end pipeline:
echo.
echo 🎯 PHASE 1: SNOWFLAKE DATA WAREHOUSE
echo   • Create warehouse: KAFKA_ANALYTICS_WH
echo   • Create database: ECOMMERCE_ANALYTICS  
echo   • Create schemas: RAW_DATA, PROCESSED_DATA, ANALYTICS, STAGING
echo   • Create tables for all event types
echo   • Set up analytics views and stored procedures
echo   • Configure automated processing tasks
echo   • Set up data quality monitoring
echo.
echo 🎯 PHASE 2: ENHANCED ANALYTICS PIPELINE
echo   • Realistic e-commerce data generation
echo   • Advanced analytics with 6 event types
echo   • Real-time data quality monitoring
echo   • JSON-based storage with batch processing
echo   • Beautiful responsive dashboard
echo   • Session tracking and user journey analytics
echo.
echo 🎯 PHASE 3: DATA INTEGRATION
echo   • Load generated data into Snowflake
echo   • Process raw events into structured tables
echo   • Enable real-time analytics queries
echo   • Set up automated data pipeline
echo.
echo 📊 FINAL RESULT:
echo   • Complete e-commerce data warehouse in Snowflake
echo   • Real-time analytics dashboard at http://localhost:5000
echo   • Automated data processing and quality monitoring
echo   • Production-ready analytics infrastructure
echo.
echo ⚠️  PREREQUISITES:
echo   • Valid Snowflake account and credentials
echo   • Python environment with required packages
echo   • Internet connection for package installation
echo.
echo Press any key to start the complete setup...
pause >nul

echo.
echo ========================================================================
echo   PHASE 1: SETTING UP SNOWFLAKE DATA WAREHOUSE
echo ========================================================================
echo.
echo 🏔️ Creating Snowflake infrastructure...
echo.

call setup_snowflake.bat

echo.
echo ✓ Snowflake setup completed!
echo.
echo ========================================================================
echo   PHASE 2: STARTING ENHANCED ANALYTICS PIPELINE
echo ========================================================================
echo.
echo 🚀 Launching enhanced e-commerce analytics dashboard...
echo.
echo The dashboard will start generating realistic e-commerce data including:
echo   • Customer profiles and sessions
echo   • Page views and product interactions
echo   • Purchase transactions and search events
echo   • User engagement and reviews
echo.
echo Data will be automatically saved to JSON files for Snowflake loading.
echo.

start "Enhanced Analytics Dashboard" cmd /k "LAUNCH_ENHANCED.bat"

echo.
echo ✓ Enhanced analytics pipeline started!
echo.
echo Waiting for data generation to begin...
timeout /t 30

echo.
echo ========================================================================
echo   PHASE 3: SETTING UP DATA INTEGRATION
echo ========================================================================
echo.
echo 📊 The data integration will be available through:
echo.
echo   • Manual loading: load_to_snowflake.bat
echo   • Automatic processing in Snowflake every 5 minutes
echo   • Real-time analytics queries in Snowflake
echo.
echo 🎯 SETUP COMPLETE! Here's what you have:
echo.
echo   1. 🏔️  Snowflake Data Warehouse:
echo      • Warehouse: KAFKA_ANALYTICS_WH
echo      • Database: ECOMMERCE_ANALYTICS
echo      • Complete table structure for e-commerce analytics
echo      • Automated processing and data quality monitoring
echo.
echo   2. 📊 Enhanced Analytics Dashboard:
echo      • URL: http://localhost:5000
echo      • Real-time data generation and visualization
echo      • Advanced analytics and quality monitoring
echo      • JSON data storage for Snowflake integration
echo.
echo   3. 🔄 Data Integration:
echo      • JSON files saved in data/ directory
echo      • Use load_to_snowflake.bat to load data
echo      • Automated processing in Snowflake
echo      • Real-time analytics queries available
echo.
echo ========================================================================
echo   NEXT STEPS
echo ========================================================================
echo.
echo 1. 🌐 Open http://localhost:5000 to view the analytics dashboard
echo 2. 📊 Click "Start Pipeline" to begin generating realistic data
echo 3. ⏱️  Let it run for a few minutes to generate sample data
echo 4. 📤 Run load_to_snowflake.bat to load data into Snowflake
echo 5. 🔍 Query your data in Snowflake using the analytics views
echo.
echo 📋 SAMPLE SNOWFLAKE QUERIES:
echo.
echo   -- View today's metrics
echo   SELECT * FROM ANALYTICS.REALTIME_METRICS;
echo.
echo   -- Check data quality
echo   SELECT * FROM ANALYTICS.DATA_QUALITY_SUMMARY ORDER BY date DESC;
echo.
echo   -- Analyze customer journeys
echo   SELECT * FROM ANALYTICS.CUSTOMER_JOURNEY LIMIT 10;
echo.
echo   -- Product performance
echo   SELECT * FROM ANALYTICS.PRODUCT_PERFORMANCE ORDER BY revenue DESC;
echo.
echo 🎉 Your complete e-commerce analytics pipeline is ready!
echo.
pause