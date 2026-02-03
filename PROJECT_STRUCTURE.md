# 🚀 Kafka E-commerce Analytics Pipeline - Project Structure

## 📁 **Core Application Files**

### **Main Dashboard**
- `multi_dashboard_app.py` - **Main Kafka-enabled dashboard application**
- `run_multi_dashboard.bat` - **Start the main dashboard**

### **Data Processing Pipeline**
- `producer.py` - **Kafka event producer**
- `stream_processor.py` - **Kafka stream processor**
- `data_generators.py` - **E-commerce data generators**

### **Storage & ETL**
- `multi_storage_manager.py` - **Multi-storage manager (Parquet + DuckDB)**
- `parquet_storage.py` - **Parquet file storage**
- `etl_pipeline.py` - **ETL pipeline for Snowflake**

### **Snowflake Integration**
- `snowflake_consumer.py` - **Snowflake consumer**
- `snowflake_loader.py` - **Snowflake data loader**
- `setup_snowflake.py` - **Snowflake setup script**
- `snowflake_setup.sql` - **Snowflake database schema**
- `snowflake_config.json` - **Snowflake configuration**
- `SNOWFLAKE_SETUP_GUIDE.md` - **Snowflake setup guide**

## 📁 **Configuration & Setup**

### **Main Setup Scripts**
- `COMPLETE_KAFKA_SETUP.bat` - **🎯 Complete Kafka pipeline setup**
- `COMPLETE_MULTI_SETUP.bat` - **🎯 Complete multi-dashboard setup**
- `COMPLETE_SETUP.bat` - **🎯 Complete system setup**

### **Individual Component Scripts**
- `run_kafka_producer.bat` - **Start Kafka producer**
- `run_stream_processor.bat` - **Start stream processor**
- `run_kafka_dashboard.bat` - **Start Kafka dashboard**
- `run_snowflake_consumer.bat` - **Start Snowflake consumer**

### **Kafka Infrastructure**
- `start_kafka_docker.bat` - **Start Kafka with Docker**
- `stop_kafka_docker.bat` - **Stop Kafka Docker containers**
- `docker-compose-kafka.yml` - **Docker Compose for Kafka**
- `setup_kafka.bat` - **Kafka setup script**

### **Project Configuration**
- `requirements.txt` - **Python dependencies**
- `README.md` - **Project documentation**

## 📁 **Directories**

### **Core Directories**
- `templates/` - **HTML templates for dashboards**
- `static/` - **Static web assets (CSS, JS)**
- `data/` - **Generated data files (Parquet, DuckDB)**
- `kafka_venv/` - **Python virtual environment**

### **Kafka Installation**
- `k/` - **Local Kafka installation (short path)**
- `kafka/` - **Original Kafka download location**

### **Development & Testing**
- `extra/` - **🗂️ All test files, demos, and alternative implementations**

## 🚀 **Quick Start Commands**

### **Start Complete Pipeline**
```bash
# Option 1: Complete Kafka-enabled setup
COMPLETE_KAFKA_SETUP.bat

# Option 2: Multi-dashboard setup
COMPLETE_MULTI_SETUP.bat

# Option 3: Basic setup
COMPLETE_SETUP.bat
```

### **Individual Components**
```bash
# Start main dashboard
run_multi_dashboard.bat

# Start Kafka producer
run_kafka_producer.bat

# Start stream processor
run_stream_processor.bat
```

### **Dashboard URLs**
- **Main Dashboard:** http://localhost:5004
- **Kafka Dashboard:** http://localhost:5004/kafka ⭐
- **Parquet Dashboard:** http://localhost:5004/parquet
- **DuckDB Dashboard:** http://localhost:5004/duckdb
- **ETL Dashboard:** http://localhost:5004/etl
- **Snowflake Dashboard:** http://localhost:5004/snowflake

## 🎯 **Key Features**

✅ **Real-time Kafka event streaming**  
✅ **Multi-storage architecture (Parquet + DuckDB)**  
✅ **ETL pipeline with Snowflake integration**  
✅ **6 specialized dashboards**  
✅ **WebSocket real-time updates**  
✅ **Comprehensive event generation**  
✅ **Data quality monitoring**  

## 📊 **Data Flow**

```
Producer → raw_events → Stream Processor → clean_events → Multi-Storage → ETL → Snowflake → Analytics
```

---
**🎉 Your complete Kafka-enabled e-commerce analytics pipeline is ready!**