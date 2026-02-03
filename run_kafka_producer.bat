@echo off
cls
echo.
echo ========================================
echo   Enhanced Kafka Producer
echo ========================================
echo.
echo 🚀 Starting enhanced Kafka producer for e-commerce events...
echo.
echo Features:
echo ✨ Realistic e-commerce event generation
echo 📊 6 event types: PAGE_VIEW, PRODUCT_VIEW, ADD_TO_CART, PURCHASE, SEARCH, REVIEW
echo 🎯 15%% invalid events for data quality testing
echo 📈 Variable event rates based on event type
echo 🔄 Comprehensive event metadata
echo.
echo Target Topic: raw_events
echo Bootstrap Servers: localhost:9092
echo.
echo Press Ctrl+C to stop the producer
echo.
pause

call kafka_venv\Scripts\activate.bat
echo.
echo Starting Enhanced Kafka Producer...
python producer.py