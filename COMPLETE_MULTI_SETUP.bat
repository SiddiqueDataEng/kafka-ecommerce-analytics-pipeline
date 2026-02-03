@echo off
cls
echo.
echo  ███╗   ███╗██╗   ██╗██╗  ████████╗██╗      ███████╗████████╗ ██████╗ ██████╗  █████╗  ██████╗ ███████╗
echo  ████╗ ████║██║   ██║██║  ╚══██╔══╝██║      ██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔══██╗██╔════╝ ██╔════╝
echo  ██╔████╔██║██║   ██║██║     ██║   ██║█████╗███████╗   ██║   ██║   ██║██████╔╝███████║██║  ███╗█████╗  
echo  ██║╚██╔╝██║██║   ██║██║     ██║   ██║╚════╝╚════██║   ██║   ██║   ██║██╔══██╗██╔══██║██║   ██║██╔══╝  
echo  ██║ ╚═╝ ██║╚██████╔╝███████╗██║   ██║      ███████║   ██║   ╚██████╔╝██║  ██║██║  ██║╚██████╔╝███████╗
echo  ╚═╝     ╚═╝ ╚═════╝ ╚══════╝╚═╝   ╚═╝      ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝
echo.
echo  🚀 COMPLETE MULTI-STORAGE E-COMMERCE ANALYTICS PIPELINE
echo.
echo ========================================================================
echo   Advanced Data Pipeline with Multiple Storage Systems & ETL
echo ========================================================================
echo.
echo 🎯 SYSTEM ARCHITECTURE:
echo.
echo   📊 DATA GENERATION
echo   • Realistic e-commerce events (customers, products, sessions)
echo   • 6 event types: page views, purchases, searches, interactions, engagement
echo   • Geographic and demographic data modeling
echo   • Data quality issues simulation (15%% error rate)
echo.
echo   💾 MULTI-STORAGE SYSTEM
echo   • 📁 Parquet Files: Columnar storage for analytics
echo   • 🦆 DuckDB: In-process analytical database
echo   • Random 50/50 distribution between storage types
echo   • Batch processing with configurable sizes
echo.
echo   🔄 ETL PIPELINE
echo   • Extract from both Parquet and DuckDB
echo   • Transform with data cleaning and validation
echo   • Deduplicate records based on event_id
echo   • Load into Snowflake data warehouse
echo   • Comprehensive error handling and logging
echo.
echo   ❄️  SNOWFLAKE DATA WAREHOUSE
echo   • Enterprise-grade data warehouse
echo   • Structured tables for all event types
echo   • Real-time analytics views
echo   • Automated processing tasks
echo   • Data quality monitoring
echo.
echo   📊 MULTI-DASHBOARD UI
echo   • 🏠 Main: Overall pipeline monitoring
echo   • 📁 Parquet: File-based storage analytics
echo   • 🦆 DuckDB: Database storage monitoring
echo   • 🔄 ETL: Pipeline execution and logs
echo   • ❄️  Snowflake: Data warehouse analytics
echo.
echo ========================================================================
echo   SETUP PHASES
echo ========================================================================
echo.
echo Phase 1: Snowflake Data Warehouse Setup
echo Phase 2: Multi-Storage Dashboard Launch
echo Phase 3: Data Generation and Processing
echo Phase 4: ETL Pipeline Integration
echo.
echo ⚠️  PREREQUISITES:
echo   • Valid Snowflake account (configured in snowflake_config.json)
echo   • Python environment with virtual environment
echo   • Sufficient disk space for data files
echo.
echo Press any key to start the complete multi-storage setup...
pause >nul

echo.
echo ========================================================================
echo   PHASE 1: SNOWFLAKE DATA WAREHOUSE SETUP
echo ========================================================================
echo.
echo 🏔️ Setting up Snowflake infrastructure...
echo   • Creating warehouse, database, and schemas
echo   • Setting up tables for all event types
echo   • Configuring analytics views and procedures
echo   • Setting up automated processing tasks
echo.

call setup_snowflake.bat

echo.
echo ✅ Phase 1 Complete: Snowflake data warehouse ready!
echo.
echo ========================================================================
echo   PHASE 2: MULTI-STORAGE DASHBOARD LAUNCH
echo ========================================================================
echo.
echo 🚀 Launching multi-dashboard application...
echo   • Installing required dependencies (DuckDB, Pandas, PyArrow)
echo   • Initializing multi-storage manager
echo   • Starting Flask application with 5 dashboards
echo   • Setting up real-time WebSocket connections
echo.

start "Multi-Storage Dashboard" cmd /k "run_multi_dashboard.bat"

echo.
echo ✅ Phase 2 Complete: Multi-dashboard application started!
echo.
echo Waiting for application to initialize...
timeout /t 15

echo.
echo ========================================================================
echo   PHASE 3: DATA GENERATION STATUS
echo ========================================================================
echo.
echo 📊 Data generation will begin automatically when you:
echo   1. Open http://localhost:5000 (Main Dashboard)
echo   2. Click "Start Pipeline" button
echo   3. Monitor real-time data flow across storage systems
echo.
echo The system will:
echo   • Generate realistic e-commerce events
echo   • Randomly distribute data between Parquet and DuckDB (50/50)
echo   • Process data in configurable batches
echo   • Maintain data quality metrics
echo.
echo ========================================================================
echo   PHASE 4: ETL PIPELINE INTEGRATION
echo ========================================================================
echo.
echo 🔄 ETL Pipeline is ready for execution:
echo   • Manual execution via ETL Dashboard
echo   • Automatic deduplication of records
echo   • Data quality validation and reporting
echo   • Snowflake integration with structured tables
echo.
echo To run ETL:
echo   1. Navigate to http://localhost:5000/etl
echo   2. Click "Run ETL" button
echo   3. Monitor processing logs and statistics
echo   4. View results in Snowflake Dashboard
echo.
echo ========================================================================
echo   🎉 COMPLETE SETUP FINISHED!
echo ========================================================================
echo.
echo 🌐 DASHBOARD URLS:
echo   • Main Dashboard:      http://localhost:5000
echo   • Parquet Dashboard:   http://localhost:5000/parquet
echo   • DuckDB Dashboard:    http://localhost:5000/duckdb
echo   • ETL Dashboard:       http://localhost:5000/etl
echo   • Snowflake Dashboard: http://localhost:5000/snowflake
echo.
echo 📋 NEXT STEPS:
echo   1. Open the Main Dashboard to start data generation
echo   2. Monitor data distribution across Parquet and DuckDB
echo   3. Run ETL pipeline to load data into Snowflake
echo   4. Explore analytics in the Snowflake Dashboard
echo   5. Use individual dashboards for detailed monitoring
echo.
echo 🔧 SYSTEM FEATURES:
echo   • Real-time event generation with quality simulation
echo   • Multi-storage architecture (Parquet + DuckDB)
echo   • ETL pipeline with deduplication and validation
echo   • Enterprise data warehouse (Snowflake)
echo   • 5 specialized monitoring dashboards
echo   • WebSocket real-time updates
echo   • Comprehensive logging and error handling
echo.
echo 📊 DATA FLOW:
echo   Events → [Parquet Files + DuckDB] → ETL Pipeline → Snowflake → Analytics
echo.
echo Your advanced multi-storage e-commerce analytics pipeline is ready!
echo.
pause