from confluent_kafka import Producer
import json, csv, time, os

TOPIC = "ecommerce.events"
BOOTSTRAP = "localhost:9092"
DATA_FILE = r"E:\Masters\ProjectBDA\EcommerceMultiStore\data\2019-Nov.csv"

TARGET_SIZE = 2 * 1024 * 1024 * 1024

producer_conf = {
    'bootstrap.servers': BOOTSTRAP,
    'linger.ms': 20,
    'batch.num.messages': 10000,
    'queue.buffering.max.messages': 500000,
    'compression.type': 'lz4',
    'acks': 0
}

producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record: {err}")

start = time.time()
count = 0
batch_size = 10000
batch = []
bytes_sent = 0

with open(DATA_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        row["product_id"] = int(row["product_id"])
        row["category_id"] = str(row["category_id"])
        row["user_id"] = int(row["user_id"])
        row["price"] = float(row["price"]) if row["price"] else 0.0

        msg = json.dumps(row).encode('utf-8')
        batch.append(msg)
        bytes_sent += len(msg)

        count += 1

        # Stop when ~2 GB reached
        if bytes_sent >= TARGET_SIZE:
            print(f"\n Reached 2 GB limit at {count:,} records.")
            break

        # Send in batches
        if len(batch) >= batch_size:
            for m in batch:
                producer.produce(TOPIC, m, callback=delivery_report)
            producer.flush()
            batch.clear()
            print(f"Sent {count:,} records... (~{bytes_sent / (1024**3):.2f} GB)")

# Send remaining messages
if batch:
    for m in batch:
        producer.produce(TOPIC, m, callback=delivery_report)
    producer.flush()

print(f"\n Finished producing {count:,} records ({bytes_sent / (1024**3):.2f} GB) in {time.time() - start:.1f}s")
