@echo off
cls
echo.
echo  ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗     ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗
echo  ██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗    ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║
echo  █████╔╝ ███████║█████╗  █████╔╝ ███████║    ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║
echo  ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║    ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║
echo  ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║    ███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║
echo  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝    ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝
echo.
echo  🚀 COMPLETE KAFKA-ENABLED E-COMMERCE ANALYTICS PIPELINE
echo.
echo ========================================================================
echo   Advanced Real-time Data Pipeline with Apache Kafka Integration
echo ========================================================================
echo.
echo 🎯 SYSTEM ARCHITECTURE:
echo.
echo   📡 KAFKA STREAMING LAYER
echo   • Real-time event ingestion and processing
echo   • Producer: Enhanced e-commerce event generation
echo   • Stream Processor: Data validation and enrichment
echo   • Consumer: Multi-storage distribution
echo   • Topics: raw_events, clean_events
echo.
echo   📊 DATA GENERATION
echo   • 6 realistic event types: PAGE_VIEW, PRODUCT_VIEW, ADD_TO_CART, PURCHASE, SEARCH, REVIEW
echo   • Comprehensive event metadata and business context
echo   • 15%% invalid events for data quality testing
echo   • Variable event rates based on business logic
echo.
echo   💾 MULTI-STORAGE SYSTEM
echo   • 📁 Parquet Files: Columnar storage for analytics
echo   • 🦆 DuckDB: In-process analytical database
echo   • Random 50/50 distribution between storage types
echo   • Batch processing with configurable sizes
echo.
echo   🔄 ETL PIPELINE
echo   • Extract from Kafka topics, Parquet files, and DuckDB
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
echo   • 🏠 Main: Overall pipeline monitoring with Kafka integration
echo   • 📡 Kafka: Real-time event streaming with terminal view
echo   • 📁 Parquet: File-based storage analytics
echo   • 🦆 DuckDB: Database storage monitoring
echo   • 🔄 ETL: Pipeline execution and logs
echo   • ❄️  Snowflake: Data warehouse analytics
echo.
echo ========================================================================
echo   SETUP PHASES
echo ========================================================================
echo.
echo Phase 1: Kafka Dashboard Launch
echo Phase 2: Stream Processor Setup
echo Phase 3: Enhanced Producer Setup
echo Phase 4: Complete Pipeline Integration
echo.
echo ⚠️  PREREQUISITES:
echo   • Apache Kafka running on localhost:9092
echo   • Valid Snowflake account (configured in snowflake_config.json)
echo   • Python environment with virtual environment
echo   • Sufficient disk space for data files
echo.
echo 📋 KAFKA TOPICS REQUIRED:
echo   • raw_events (for incoming events)
echo   • clean_events (for processed events)
echo.
echo Press any key to start the complete Kafka-enabled setup...
pause >nul

echo.
echo ========================================================================
echo   PHASE 1: KAFKA DASHBOARD LAUNCH
echo ========================================================================
echo.
echo 🚀 Launching Kafka-enabled multi-dashboard application...
echo   • Real-time Kafka event streaming
echo   • Terminal-style event monitoring
echo   • WebSocket-based live updates
echo   • Integrated producer and consumer controls
echo.

start "Kafka Multi-Dashboard" cmd /k "run_kafka_dashboard.bat"

echo.
echo ✅ Phase 1 Complete: Kafka dashboard application started!
echo.
echo Waiting for application to initialize...
timeout /t 15

echo.
echo ========================================================================
echo   PHASE 2: STREAM PROCESSOR SETUP
echo ========================================================================
echo.
echo 🔄 Setting up enhanced stream processor...
echo   • Comprehensive event validation
echo   • Business value scoring
echo   • Event categorization and enrichment
echo   • Real-time data quality monitoring
echo.

start "Stream Processor" cmd /k "run_stream_processor.bat"

echo.
echo ✅ Phase 2 Complete: Stream processor ready!
echo.
echo Waiting for processor to initialize...
timeout /t 10

echo.
echo ========================================================================
echo   PHASE 3: ENHANCED PRODUCER SETUP
echo ========================================================================
echo.
echo 📡 Setting up enhanced Kafka producer...
echo   • Realistic e-commerce event generation
echo   • 6 comprehensive event types
echo   • Variable event rates and business logic
echo   • Data quality simulation (15%% invalid events)
echo.

start "Kafka Producer" cmd /k "run_kafka_producer.bat"

echo.
echo ✅ Phase 3 Complete: Enhanced producer ready!
echo.
echo Waiting for producer to start generating events...
timeout /t 10

echo.
echo ========================================================================
echo   PHASE 4: COMPLETE PIPELINE INTEGRATION
echo ========================================================================
echo.
echo 🎯 Complete Kafka streaming pipeline is now operational:
echo.
echo 📡 KAFKA FLOW:
echo   Producer → raw_events → Stream Processor → clean_events → Multi-Storage
echo.
echo 💾 STORAGE FLOW:
echo   Kafka Events → [Parquet Files + DuckDB] → ETL Pipeline → Snowflake
echo.
echo 📊 MONITORING:
echo   Real-time dashboards with live event streaming and terminal monitoring
echo.
echo ========================================================================
echo   🎉 COMPLETE KAFKA SETUP FINISHED!
echo ========================================================================
echo.
echo 🌐 DASHBOARD URLS:
echo   • Main Dashboard:      http://localhost:5004
echo   • Kafka Dashboard:     http://localhost:5004/kafka  ⭐ NEW!
echo   • Parquet Dashboard:   http://localhost:5004/parquet
echo   • DuckDB Dashboard:    http://localhost:5004/duckdb
echo   • ETL Dashboard:       http://localhost:5004/etl
echo   • Snowflake Dashboard: http://localhost:5004/snowflake
echo.
echo 📋 NEXT STEPS:
echo   1. Open the Kafka Dashboard to see real-time event streaming
echo   2. Monitor the terminal view for live event processing
echo   3. Use dashboard controls to start/stop Kafka components
echo   4. Run ETL pipeline to load data into Snowflake
echo   5. Explore analytics in the Snowflake Dashboard
echo.
echo 🔧 KAFKA COMPONENTS RUNNING:
echo   • ✅ Enhanced Producer: Generating realistic e-commerce events
echo   • ✅ Stream Processor: Validating and enriching events
echo   • ✅ Multi-Dashboard: Real-time monitoring and control
echo   • ✅ Multi-Storage: Distributing events to Parquet and DuckDB
echo.
echo 📊 DATA FLOW:
echo   Events → Kafka → Multi-Storage → ETL → Snowflake → Analytics
echo.
echo 🎯 FEATURES AVAILABLE:
echo   • Real-time event generation with business logic
echo   • Live event streaming with terminal monitoring
echo   • Data quality validation and scoring
echo   • Multi-storage architecture (Parquet + DuckDB)
echo   • ETL pipeline with deduplication
echo   • Enterprise data warehouse (Snowflake)
echo   • 6 specialized monitoring dashboards
echo   • WebSocket real-time updates
echo   • Comprehensive logging and error handling
echo.
echo Your complete Kafka-enabled e-commerce analytics pipeline is ready!
echo.
pause