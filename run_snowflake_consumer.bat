@echo off
echo Activating virtual environment and running Snowflake consumer...
echo Note: Make sure to update Snowflake credentials in snowflake_consumer.py
call kafka_venv\Scripts\activate.bat
python snowflake_consumer.py