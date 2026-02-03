@echo off
cls
echo.
echo ========================================
echo   Multi-Storage Analytics Dashboard
echo ========================================
echo.
echo 🚀 Starting the complete multi-storage e-commerce analytics pipeline...
echo.
echo Features:
echo ✨ Multi-storage data generation (Parquet + DuckDB)
echo 🔄 ETL pipeline with deduplication
echo ❄️  Snowflake data warehouse integration
echo 📊 5 specialized dashboards
echo 🎯 Real-time analytics and monitoring
echo.
echo Dashboards Available:
echo 🏠 Main Dashboard: http://localhost:5004
echo 📁 Parquet Dashboard: http://localhost:5004/parquet
echo 🦆 DuckDB Dashboard: http://localhost:5004/duckdb
echo 🔄 ETL Dashboard: http://localhost:5004/etl
echo ❄️  Snowflake Dashboard: http://localhost:5004/snowflake
echo.
echo Note: The application will start automatically...
echo Press Ctrl+C to stop the application when done.
echo.
pause

call kafka_venv\Scripts\activate.bat
echo.
echo Starting Multi-Dashboard Application...
python multi_dashboard_app.py