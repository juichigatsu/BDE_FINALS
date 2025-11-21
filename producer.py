import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from pymongo import MongoClient
import argparse

# kafka producer
# ---------------------------------------------------------
def create_kafka_producer(broker):
    return KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

# mongodb client (wala na hadoop)
# ---------------------------------------------------------
def create_mongo_client(uri, db_name, coll_name):
    client = MongoClient(uri)
    collection = client[db_name][coll_name]
    return collection

# api data fetching
# ---------------------------------------------------------
def fetch_weather(lat, lon):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true"
    response = requests.get(url, timeout=5)
    data = response.json()
    value = data["current_weather"]["temperature"]
    return value

# producer logic (main)
# ---------------------------------------------------------
def run_producer(broker, topic, mongo_uri, db, coll, lat, lon, interval):
    producer = create_kafka_producer(broker)
    collection = create_mongo_client(mongo_uri, db, coll)

    print(f"Producer started. Sending data to Kafka topic '{topic}' and MongoDB '{db}.{coll}'...")
    print("Press CTRL+C to stop.\n")

    while True:
        try:
            value = fetch_weather(lat, lon)

            doc = {
                "timestamp": datetime.utcnow().isoformat(),
                "value": value,
                "metric_type": "temperature",
                "sensor_id": "sensor_1"
            }

            # >kafka
            producer.send(topic, doc)

            # w > mongodb
            collection.insert_one(doc)

            print(f"Sent: {doc}")
            time.sleep(interval)

        except KeyboardInterrupt:
            print("\nProducer stopped by user.")
            break

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

# CLI parse
# ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka + MongoDB Data Producer")

    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="streaming-data", help="Kafka topic")

    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017/", help="MongoDB URI")
    parser.add_argument("--db", default="streamingdb", help="MongoDB database name")
    parser.add_argument("--coll", default="historical_data", help="MongoDB collection name")

    parser.add_argument("--lat", type=float, default=14.5995, help="Latitude for API")
    parser.add_argument("--lon", type=float, default=120.9842, help="Longitude for API")

    parser.add_argument("--interval", type=int, default=15, help="Seconds between messages")

    args = parser.parse_args()

    run_producer(
        broker=args.broker,
        topic=args.topic,
        mongo_uri=args.mongo_uri,
        db=args.db,
        coll=args.coll,
        lat=args.lat,
        lon=args.lon,
        interval=args.interval
    )
