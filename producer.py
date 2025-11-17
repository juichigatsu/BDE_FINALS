import os
import time
import json
import argparse
import tempfile
import subprocess
from datetime import datetime

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Optional: use pyarrow for HDFS writes
try:
    import pyarrow as pa
    import pyarrow.fs as pafs
    PYARROW_AVAILABLE = True
except Exception:
    PYARROW_AVAILABLE = False

DEFAULT_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "streaming-data")
DEFAULT_HDFS_DIR = os.environ.get("HDFS_DIR", "/user/streaming/dashboard")
DEFAULT_LAT = os.environ.get("LAT", "14.5995")
DEFAULT_LON = os.environ.get("LON", "120.9842")
DEFAULT_INTERVAL = int(os.environ.get("INTERVAL", "15"))
FLUSH_TO_HDFS_EVERY = 10

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_current_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current_weather": "true",
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def build_message(lat, lon, api_result):
    now_iso = datetime.utcnow().isoformat() + "Z"
    current = api_result.get("current_weather", {})
    temp = current.get("temperature")

    return {
        "timestamp": now_iso,
        "value": float(temp) if temp is not None else None,
        "metric_type": "temperature",
        "sensor_id": f"open-meteo_{lat}_{lon}"
    }

def write_batch_to_hdfs_jsonlines(batch, hdfs_dir):
    if not batch:
        return

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as tmp:
        local_path = tmp.name
        for obj in batch:
            tmp.write(json.dumps(obj) + "\n")

    if PYARROW_AVAILABLE:
        try:
            hdfs = pafs.HadoopFileSystem()
            filename = f"{hdfs_dir.rstrip('/')}/stream_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
            with hdfs.open_output_stream(filename) as out:
                with open(local_path, "rb") as f:
                    out.write(f.read())
            print(f"[hdfs] Wrote {len(batch)} records via pyarrow: {filename}")
            os.remove(local_path)
            return
        except Exception as e:
            print(f"[hdfs] pyarrow error: {e}")

    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=False)
        remote_path = f"{hdfs_dir.rstrip('/')}/stream_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jsonl"
        subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, remote_path], check=True)
        print(f"[hdfs] Wrote {len(batch)} records via CLI: {remote_path}")
        os.remove(local_path)
        return
    except Exception as e:
        print(f"[hdfs] CLI write failed: {e} - file kept at {local_path}")

def run_producer(broker, topic, hdfs_dir, lat, lon, interval):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5
    )

    print(f"Producer running → broker={broker}, topic={topic}, HDFS={hdfs_dir}")

    counter = 0
    batch = []

    try:
        while True:
            try:
                api_resp = fetch_current_weather(lat, lon)
            except Exception as e:
                print(f"[api] failed → {e}")
                time.sleep(interval)
                continue

            msg = build_message(lat, lon, api_resp)
            if msg["value"] is None:
                print("[warn] no temperature — skipping")
                time.sleep(interval)
                continue

            try:
                future = producer.send(topic, value=msg)
                result = future.get(timeout=10)
                print(f"[kafka] sent {msg}")
            except KafkaError as e:
                print(f"[kafka] error → {e}")

            batch.append(msg)
            counter += 1

            if counter % FLUSH_TO_HDFS_EVERY == 0:
                write_batch_to_hdfs_jsonlines(batch, hdfs_dir)
                batch = []

            time.sleep(interval)

    except KeyboardInterrupt:
        print("Stopping producer...")
        if batch:
            write_batch_to_hdfs_jsonlines(batch, hdfs_dir)
        producer.flush()
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--hdfs-dir", default=DEFAULT_HDFS_DIR)
    parser.add_argument("--lat", default=DEFAULT_LAT)
    parser.add_argument("--lon", default=DEFAULT_LON)
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL)
    args = parser.parse_args()

    run_producer(args.broker, args.topic, args.hdfs-dir, args.lat, args.lon, args.interval)



