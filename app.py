import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh
import subprocess
import json
import io

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker",
        value="localhost:9092",
        help="Kafka broker address"
    )

    kafka_topic = st.sidebar.text_input(
        "Kafka Topic",
        value="streaming-data",
        help="Kafka topic to consume from"
    )

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS"],
        help="Historical storage system"
    )

    st.sidebar.subheader("HDFS Settings")
    hdfs_dir = st.sidebar.text_input(
        "HDFS Directory",
        value="/user/streaming/dashboard",
        help="Directory where producer stores history"
    )
    st.session_state['hdfs_dir'] = hdfs_dir

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

def generate_sample_data():
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]

    return pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['sensor_1'] * 100
    })

def consume_kafka_data(config):
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")

    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=[kafka_broker],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=5000
            )
        except Exception as e:
            st.error(f"Kafka connection failed: {e}")
            st.session_state[cache_key] = None

    consumer = st.session_state[cache_key]
    if not consumer:
        return generate_sample_data()

    try:
        messages = []
        start_time = time.time()

        while time.time() - start_time < 5 and len(messages) < 10:
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, batch in msg_pack.items():
                for msg in batch:
                    data = msg.value
                    try:
                        timestamp = data['timestamp']
                        if timestamp.endswith('Z'):
                            timestamp = timestamp[:-1] + '+00:00'
                        data['timestamp'] = datetime.fromisoformat(timestamp)
                        messages.append(data)
                    except:
                        continue

        if messages:
            return pd.DataFrame(messages)
        return generate_sample_data()

    except Exception as e:
        st.warning(f"Kafka error: {e}")
        return generate_sample_data()

def query_historical_data(time_range="1h", metrics=None):
    st.info("Reading historical data from HDFS...")

    hdfs_dir = st.session_state.get("hdfs_dir", "/user/streaming/dashboard")
    records = []

    # Try CLI first
    try:
        cmd = ["hdfs", "dfs", "-cat", f"{hdfs_dir}/*"]
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if p.returncode == 0:
            for line in p.stdout.splitlines():
                if line.strip():
                    try:
                        records.append(json.loads(line))
                    except:
                        pass
        else:
            st.warning("No HDFS files found — showing sample data instead.")
            return generate_sample_data()

    except FileNotFoundError:
        st.error("HDFS CLI not found — cannot read historical data.")
        return generate_sample_data()

    if not records:
        st.info("No history yet — waiting for producer to write files.")
        return generate_sample_data()

    df = pd.DataFrame(records)
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    now = pd.Timestamp.utcnow()
    if time_range.endswith('h'):
        df = df[df['timestamp'] >= now - pd.Timedelta(hours=int(time_range[:-1]))]
    elif time_range.endswith('d'):
        df = df[df['timestamp'] >= now - pd.Timedelta(days=int(time_range[:-1]))]

    if metrics:
        df = df[df['metric_type'].isin(metrics)]

    df = df.sort_values('timestamp')

    return df

def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")

    refresh_state = st.session_state.refresh_state
    st.info(f"Auto-refresh: {'🟢 On' if refresh_state['auto_refresh'] else '🔴 Off'} | Interval: {refresh_interval}s")

    with st.spinner("Fetching real-time data..."):
        real_time_data = consume_kafka_data(config)

    if real_time_data is None or real_time_data.empty:
        st.warning("No data found.")
        return

    data_freshness = datetime.now() - refresh_state['last_refresh']
    freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡"

    st.success(f"{freshness_color} Updated {data_freshness.total_seconds():.0f}s ago")

    st.subheader("Live Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Records", len(real_time_data))
    col2.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
    col3.metric("Time Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")

    st.subheader("📈 Real-time Trend")
    fig = px.line(
        real_time_data,
        x="timestamp",
        y="value",
        title="Live Sensor Stream",
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Raw Data"):
        st.dataframe(real_time_data.sort_values('timestamp', ascending=False), height=300)

def display_historical_view(config):
    st.header("📊 Historical Data")

    st.subheader("Filters")
    col1, col2, col3 = st.columns(3)

    time_range = col1.selectbox("Time Range", ["1h", "24h", "7d", "30d"])
    metric_type = col2.selectbox("Metric Type", ["temperature", "all"])
    aggregation = col3.selectbox("Aggregation", ["raw", "hourly", "daily"])

    metrics = [metric_type] if metric_type != "all" else None

    historical_data = query_historical_data(time_range, metrics)

    st.subheader("Historical Table")
    st.dataframe(historical_data, hide_index=True)

    st.subheader("Trend")
    fig = px.line(historical_data, x="timestamp", y="value", title="Historical Trend")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Stats")
    col1, col2 = st.columns(2)
    col1.metric("Total Records", len(historical_data))
    col1.metric("Average Value", f"{historical_data['value'].mean():.2f}")
    col2.metric("Min Value", f"{historical_data['value'].min():.2f}")
    col2.metric("Max Value", f"{historical_data['value'].max():.2f}")

def main():
    st.title("🚀 Streaming Data Dashboard")

    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=True
    )

    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 15)

    if st.session_state.refresh_state['auto_refresh']:
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()

    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])

    with tab1:
        display_real_time_view(config, refresh_interval)

    with tab2:
        display_historical_view(config)

if __name__ == "__main__":
    main()



