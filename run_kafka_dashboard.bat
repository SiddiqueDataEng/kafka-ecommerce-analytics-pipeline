@echo off
cls
echo.
echo ========================================
echo   Kafka Multi-Storage Dashboard
echo ========================================
echo.
echo 🚀 Starting Kafka-enabled multi-storage analytics dashboard...
echo.
echo Features:
echo 📡 Real-time Kafka event streaming (when Kafka available)
echo 💾 Multi-storage (Parquet + DuckDB)
echo 🔄 ETL pipeline with deduplication
echo ❄️  Snowflake data warehouse integration
echo 📊 6 specialized dashboards
echo 🎯 Real-time analytics and monitoring
echo 🖥️  Terminal-style event monitoring
echo.
echo Dashboards Available:
echo 🏠 Main Dashboard: http://localhost:5004
echo 📡 Kafka Dashboard: http://localhost:5004/kafka
echo 📁 Parquet Dashboard: http://localhost:5004/parquet
echo 🦆 DuckDB Dashboard: http://localhost:5004/duckdb
echo 🔄 ETL Dashboard: http://localhost:5004/etl
echo ❄️  Snowflake Dashboard: http://localhost:5004/snowflake
echo.
echo Note: Kafka features will be enabled if Kafka is running on localhost:9092
echo.
pause

call kafka_venv\Scripts\activate.bat
echo.
echo Starting Kafka Multi-Dashboard Application...
python multi_dashboard_app.py