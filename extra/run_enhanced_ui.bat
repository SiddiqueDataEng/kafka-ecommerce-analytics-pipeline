@echo off
echo ========================================
echo   Enhanced E-commerce Analytics Pipeline
echo ========================================
echo.
echo Features:
echo ✨ Realistic e-commerce data generation
echo 📊 Advanced analytics and metrics
echo 💾 Parquet file storage
echo ❄️  Snowflake integration
echo 🎯 Data quality monitoring
echo 📈 Enhanced visualizations
echo.
echo Dashboard will be available at: http://localhost:5000
echo.
echo Note: To enable Snowflake integration, update credentials in app_enhanced.py
echo.
call kafka_venv\Scripts\activate.bat
python app_enhanced.py