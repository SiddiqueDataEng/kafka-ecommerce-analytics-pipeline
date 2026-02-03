@echo off
cls
echo.
echo ========================================
echo   Enhanced Stream Processor
echo ========================================
echo.
echo 🔄 Starting enhanced Kafka stream processor...
echo.
echo Features:
echo ✅ Comprehensive event validation
echo 🎯 Business value scoring
echo 📊 Event categorization (engagement, consideration, conversion, retention)
echo 🔍 Data quality monitoring
echo 📈 Session context enrichment
echo ⚡ Real-time processing
echo.
echo Input Topic: raw_events
echo Output Topic: clean_events
echo Consumer Group: enhanced-stream-processor
echo Bootstrap Servers: localhost:9092
echo.
echo Press Ctrl+C to stop the processor
echo.
pause

call kafka_venv\Scripts\activate.bat
echo.
echo Starting Enhanced Stream Processor...
python stream_processor.py