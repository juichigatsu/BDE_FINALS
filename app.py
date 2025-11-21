import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh
import subprocess

# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------
st.set_page_config(
    page_title="Streaming Data Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ---------------------------------------------------------
# Custom UI Styling (professional pastel blue)
# ---------------------------------------------------------
st.markdown("""
    <style>
        .main {
            background-color: #f7f9fc;
        }
        section[data-testid="stSidebar"] {
            background-color: #eef2f7 !important;
        }
        h1, h2, h3, h4, h5, h6 {
            color: #1a2b49 !important;
            font-weight: 600 !important;
        }
        button[role="tab"] {
            background-color: #eef2f7 !important;
            color: #1a2b49 !important;
            border-radius: 6px !important;
            padding: 8px 16px !important;
            font-weight: 500;
        }
        button[role="tab"][aria-selected="true"] {
            background-color: #d7dfeb !important;
        }
        .stDataFrame div, .stTable td, .stTable th {
            color: #1a2b49 !important;
        }
    </style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# Sidebar: configuration panel
# ---------------------------------------------------------
def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    # Kafka connection input
    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092"
    )

    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="streaming-data"
    )

    # MongoDB configuration
    st.sidebar.subheader("MongoDB Configuration")
    mongo_uri = st.sidebar.text_input(
        "MongoDB URI",
        value="mongodb://localhost:27017/"
    )

    mongo_db = st.sidebar.text_input(
        "Database",
        value="streamingdb"
    )

    mongo_collection = st.sidebar.text_input(
        "Collection",
        value="historical_data"
    )

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri,
        "mongo_db": mongo_db,
        "mongo_collection": mongo_collection
    }

# ---------------------------------------------------------
# Real-time fallback sample data
# ---------------------------------------------------------
def generate_sample_data():
    now = datetime.now()
    times = [now - timedelta(minutes=i) for i in range(100)]

    return pd.DataFrame({
        "timestamp": times,
        "value": [100 + (i % 10) + i * 0.2 for i in range(100)],
        "metric_type": ["temperature"] * 100,
        "sensor_id": ["sensor_1"] * 100
    })

# ---------------------------------------------------------
# Kafka consumer for real-time data
# ---------------------------------------------------------
def consume_kafka_data(config):
    kafka_broker = config["kafka_broker"]
    kafka_topic = config["kafka_topic"]

    cache_key = f"kafka_{kafka_broker}_{kafka_topic}"

    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                consumer_timeout_ms=4000
            )
        except Exception:
            return generate_sample_data()

    consumer = st.session_state[cache_key]

    messages = []
    start = time.time()

    # Poll Kafka for 4 seconds or until 10 messages
    while time.time() - start < 4 and len(messages) < 10:
        batch = consumer.poll(timeout_ms=1000)
        for _, msgs in batch.items():
            for msg in msgs:
                try:
                    d = msg.value
                    d["timestamp"] = datetime.fromisoformat(
                        d["timestamp"].replace("Z", "+00:00")
                    )
                    messages.append(d)
                except:
                    pass

    if messages:
        return pd.DataFrame(messages)
    else:
        return generate_sample_data()

# ---------------------------------------------------------
# Query historical data from MongoDB
# ---------------------------------------------------------
def query_historical_data(config, time_range, metric_filter):
    try:
        client = MongoClient(config["mongo_uri"])
        collection = client[config["mongo_db"]][config["mongo_collection"]]

        # Determine time window
        now = datetime.utcnow()
        if time_range.endswith("h"):
            delta = timedelta(hours=int(time_range[:-1]))
        elif time_range.endswith("d"):
            delta = timedelta(days=int(time_range[:-1]))

        start_time = now - delta

        # MongoDB query
        query = {"timestamp": {"$gte": start_time}}
        if metric_filter != "all":
            query["metric_type"] = metric_filter

        docs = list(collection.find(query))

        if not docs:
            return generate_sample_data()

        # Convert to DataFrame
        df = pd.DataFrame(docs)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    except Exception:
        return generate_sample_data()

# ---------------------------------------------------------
# Real-time dashboard tab
# ---------------------------------------------------------
def display_real_time_view(config, refresh_interval):
    st.header("Real-Time Streaming")

    # Auto-refresh indicator
    state = st.session_state.refresh_state
    st.info(f"Auto-refresh: {state['auto_refresh']} | Interval: {refresh_interval}s")

    # Fetch Kafka data
    with st.spinner("Receiving live data..."):
        df = consume_kafka_data(config)

    # Metrics
    st.subheader("Live Metrics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Records", len(df))
    c2.metric("Latest Value", f"{df['value'].iloc[-1]:.2f}")
    c3.metric(
        "Time Range",
        f"{df['timestamp'].min().strftime('%H:%M')} - {df['timestamp'].max().strftime('%H:%M')}"
    )

    # Line graph
    fig = px.line(
        df,
        x="timestamp",
        y="value",
        title="Real-time Trend",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Raw data
    with st.expander("Raw Data"):
        st.dataframe(df, height=250)

# ---------------------------------------------------------
# Historical dashboard tab
# ---------------------------------------------------------
def display_historical_view(config):
    st.header("Historical Data")

    c1, c2 = st.columns(2)
    time_range = c1.selectbox("Time Range", ["1h", "6h", "24h", "7d"])
    metric = c2.selectbox("Metric Type", ["temperature", "all"])

    df = query_historical_data(config, time_range, metric)

    st.subheader("Historical Trend")
    fig = px.line(
        df,
        x="timestamp",
        y="value",
        title="Historical Trend",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Dataset")
    st.dataframe(df, height=300)

# ---------------------------------------------------------
# Main app
# ---------------------------------------------------------
def main():
    st.title("Streaming Data Dashboard")

    # Auto-refresh state
    if "refresh_state" not in st.session_state:
        st.session_state.refresh_state = {
            "last_refresh": datetime.now(),
            "auto_refresh": True
        }

    config = setup_sidebar()

    # Refresh controls
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state["auto_refresh"] = st.sidebar.checkbox(
        "Enable Auto Refresh", value=True
    )
    interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 15)

    if st.session_state.refresh_state["auto_refresh"]:
        st_autorefresh(interval=interval * 1000, key="auto")

    # Navigation tabs
    tab1, tab2 = st.tabs(["Real-time Streaming", "Historical Data"])

    with tab1:
        display_real_time_view(config, interval)

    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()

