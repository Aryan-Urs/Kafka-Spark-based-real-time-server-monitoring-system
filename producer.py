import csv
import json
import sys
from kafka import KafkaProducer

CSV_FILE = 'dataset.csv'
KAFKA_BOOTSTRAP_SERVER = '172.28.204.229:9092'

TOPIC_CPU = 'topic-cpu'
TOPIC_MEM = 'topic-mem'
TOPIC_NET = 'topic-net'
TOPIC_DISK = 'topic-disk'

DEFAULT_DATE = '2025-10-20'  # Change if needed

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Kafka producer connected.")
        return producer
    except Exception as e:
        print(f"❌ Error connecting to Kafka: {e}")
        sys.exit(1)

def format_ts(ts_str):
    """Ensure ts is in YYYY-MM-DD HH:MM:SS format."""
    if len(ts_str) == 8:  # HH:MM:SS only
        return f"{DEFAULT_DATE} {ts_str}"
    return ts_str

def stream_data(producer):
    try:
        with open(CSV_FILE, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for i, row in enumerate(csv_reader):
                try:
                    ts_full = format_ts(row["ts"])
                    common_data = {"ts": ts_full, "server_id": row["server_id"]}

                    producer.send(TOPIC_CPU, value={**common_data, "cpu_pct": float(row["cpu_pct"])})
                    producer.send(TOPIC_MEM, value={**common_data, "mem_pct": float(row["mem_pct"])})
                    producer.send(TOPIC_NET, value={**common_data, "net_in": float(row["net_in"]), "net_out": float(row["net_out"])})
                    producer.send(TOPIC_DISK, value={**common_data, "disk_io": float(row["disk_io"])})

                    if (i + 1) % 100 == 0:
                        print(f"📤 Sent {i+1} rows...")

                except Exception as e:
                    print(f"⚠ Error on row {i}: {row} -> {e}")

            producer.flush()
    except FileNotFoundError:
        print(f"❌ CSV file '{CSV_FILE}' not found.")
    finally:
        producer.close()
        print("✅ Kafka producer closed.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    stream_data(kafka_producer)