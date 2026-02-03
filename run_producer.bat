@echo off
echo Activating virtual environment and running producer...
call kafka_venv\Scripts\activate.bat
python producer.py