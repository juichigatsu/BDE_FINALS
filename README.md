VIEW THE FULL DOCUMENT HERE:
https://docs.google.com/document/d/1rDA7j3Es5jJAVoFW9drAd8gJyoJQkv-Lzxg9rLv76YI/edit?usp=sharing

Streaming Data Dashboard
This project implements a complete streaming analytics system using Apache Kafka, MongoDB, and Streamlit.
 It features two data pipelines: one for real-time monitoring and one for long-term historical analysis.

Architecture Overview
Real-Time Pipeline
Kafka Producer → Kafka Broker → Streamlit Dashboard (Real-Time View)
Historical Pipeline
Kafka Producer → MongoDB (Storage) → Streamlit Dashboard (Historical View)

Components
1. Producer (producer.py)
Fetches live data from a public API.


Formats each record with:


timestamp


value


metric_type


sensor_id


Sends data to Kafka.


Writes each record to MongoDB for historical storage.


2. Dashboard (app.py)
Two main views are provided:
Real-Time Streaming
Consumes messages from Kafka.


Displays:


Live metrics panel


Real-time line chart


Latest data updates


Historical Data
Reads stored messages from MongoDB.


Supports:


Time range filtering


Metric filtering


Aggregation (raw, hourly, daily)


Displays:


Historical table


Trend chart


Summary statistics



Requirements
Install dependencies using:
pip install -r requirements.txt

Setup Instructions
1. Create a Conda Environment
conda create -n bigdata python=3.10.13
 conda activate bigdata
2. Install Dependencies
pip install -r requirements.txt
3. Start Kafka
You must download and extract Kafka before running these:
Start ZooKeeper (if needed):
 bin/zookeeper-server-start.sh config/zookeeper.properties
Start Kafka broker:
 bin/kafka-server-start.sh config/server.properties

MongoDB Setup
Ensure MongoDB is running:
systemctl status mongodb
Database name used:
 streamingdb
Collection name:
 historical_data

Running the System
1. Start the Kafka Producer
python producer.py
2. Launch the Dashboard
streamlit run app.py

Project Files
File
Description
app.py
Streamlit dashboard (real-time + historical views)
producer.py
Kafka Producer + MongoDB writer
requirements.txt
Python package list
README.md
Documentation
