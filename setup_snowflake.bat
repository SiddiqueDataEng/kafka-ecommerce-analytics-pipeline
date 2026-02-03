@echo off
echo ========================================
echo   Snowflake Setup for E-commerce Analytics
echo ========================================
echo.
echo This script will create the complete Snowflake infrastructure:
echo.
echo 📊 WAREHOUSE:
echo   • KAFKA_ANALYTICS_WH (Medium, auto-suspend, auto-resume)
echo.
echo 🗄️ DATABASE:
echo   • ECOMMERCE_ANALYTICS
echo.
echo 📁 SCHEMAS:
echo   • RAW_DATA (for streaming data from Kafka)
echo   • PROCESSED_DATA (for cleaned and structured data)
echo   • ANALYTICS (for views and aggregated data)
echo   • STAGING (for temporary processing)
echo.
echo 📋 TABLES:
echo   • Raw: RAW_EVENTS, RAW_SESSIONS, RAW_CUSTOMERS
echo   • Processed: PAGE_VIEWS, PURCHASES, PRODUCT_INTERACTIONS, etc.
echo   • Analytics: Customer journey, product performance views
echo.
echo 🔧 AUTOMATION:
echo   • Stored procedures for data processing
echo   • Automated tasks for real-time processing
echo   • Data quality monitoring
echo.
echo ⚠️  IMPORTANT: Update snowflake_config.json with your credentials first!
echo.
pause

echo.
echo 🚀 Starting Snowflake setup...
echo.

call kafka_venv\Scripts\activate.bat
python setup_snowflake.py

echo.
echo Setup completed! Check the output above for any issues.
echo.
pause