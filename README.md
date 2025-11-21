VIEW THE FULL DOCUMENT HERE:
https://docs.google.com/document/d/1rDA7j3Es5jJAVoFW9drAd8gJyoJQkv-Lzxg9rLv76YI/edit?usp=sharing

Streaming Data Dashboard — Big Data Final Project
Overview
This project implements a complete real-time big data streaming pipeline using:
Apache Kafka (Producer + Consumer)


MongoDB (Historical storage)


Streamlit (Dashboard)


Python (Data processing + API integration)


The system is designed to ingest live data from an external API, visualize it in real time, and store it for historical analysis.

Architecture
1. Real-time Pipeline
API → Kafka Producer → Kafka Topic → Streamlit Consumer → Real-time Dashboard

2. Historical Pipeline
Kafka Producer → MongoDB → Streamlit Query → Historical Dashboard

The dashboard is split into two views:
View
Source
Description
Real-time Streaming
Kafka
Displays the last incoming messages live
Historical Data
MongoDB
Long-term stored records with filtering


Features
✅ Kafka Producer
Fetches live data from a public API


Sends structured messages to Kafka topic


Stores all messages into MongoDB simultaneously


✅ Streamlit Dashboard
Real-time line chart


Metrics summary boxes


Historical filtering (range, metric type, aggregation)


MongoDB querying


Auto-refresh system


✅ MongoDB Integration
Database: streamingdb


Collection: historical_data


Fast queries based on timestamps


Fully JSON-based record storage


✅ Error Handling
Kafka connection retries


Fallback sample data


MongoDB connection validation


Invalid timestamp handling



Project Structure
BDE_FINALS/
│── app.py               # Streamlit dashboard
│── producer.py          # Kafka producer + API ingestor
│── requirements.txt     # Python dependencies
│── README.md            # Documentation


Installation & Setup
1. Create Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

2. Install Requirements
pip install -r requirements.txt


Start Kafka
Open a new terminal inside the Kafka folder:
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

Open another terminal:
cd ~/kafka
bin/kafka-server-start.sh config/server.properties


Run the Producer
python3 producer.py

This begins sending real-time API data to:
Kafka topic (streaming-data)


MongoDB (streamingdb → historical_data)



Run the Dashboard
streamlit run app.py

Your browser will open automatically.

MongoDB Setup
Your MongoDB service must be running:
sudo systemctl status mongod

Database + collection automatically created:
Database: streamingdb


Collection: historical_data



Data Schema
All records follow this format:
{
  "timestamp": "2025-01-01T12:00:00Z",
  "value": 23.7,
  "metric_type": "temperature",
  "sensor_id": "sensor_1"
}

