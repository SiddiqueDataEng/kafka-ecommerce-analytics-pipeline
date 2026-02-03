# 🚀 Kafka E-commerce Analytics Pipeline 
 
**Real-time Event Streaming | Multi-Storage Architecture | Interactive Dashboards** 
 
## 🎯 Features 
 
✅ **Real-time Kafka event streaming** 
✅ **Multi-storage architecture (Parquet + DuckDB)** 
✅ **ETL pipeline with Snowflake integration** 
✅ **6 specialized dashboards with WebSocket updates** 
✅ **Comprehensive e-commerce event generation** 
✅ **Stream processing with data quality monitoring** 
 
## 🚀 Quick Start 
 
```bash 
# Complete Kafka setup 
COMPLETE_KAFKA_SETUP.bat 
 
# Start dashboard 
run_multi_dashboard.bat 
``` 
 
## 📊 Dashboards 
 
- **Kafka Dashboard:** http://localhost:5004/kafka 
- **Parquet Dashboard:** http://localhost:5004/parquet 
- **DuckDB Dashboard:** http://localhost:5004/duckdb 
- **ETL Dashboard:** http://localhost:5004/etl 
- **Snowflake Dashboard:** http://localhost:5004/snowflake 
 
## 🏗️ Architecture 
 
``` 
Producer → raw_events → Stream Processor → clean_events → Multi-Storage → ETL → Snowflake → Analytics 
``` 
 
## 🛠️ Tech Stack 
 
- **Apache Kafka** - Event streaming 
- **Python Flask** - Web framework 
- **WebSocket** - Real-time updates 
- **Parquet** - Columnar storage 
- **DuckDB** - Analytical database 
- **Snowflake** - Data warehouse 
