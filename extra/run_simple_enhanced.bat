@echo off
echo ========================================
echo   Enhanced E-commerce Analytics Pipeline
echo ========================================
echo.
echo Features:
echo ✨ Realistic e-commerce data generation
echo 📊 Advanced analytics and metrics  
echo 💾 JSON file storage (lightweight)
echo 🎯 Data quality monitoring
echo 📈 Enhanced visualizations
echo 👥 Customer and session tracking
echo 🛒 E-commerce specific events
echo.
echo Dashboard will be available at: http://localhost:5000
echo.
call kafka_venv\Scripts\activate.bat
python simple_enhanced_app.py