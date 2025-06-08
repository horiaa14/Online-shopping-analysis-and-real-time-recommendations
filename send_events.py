import csv
import json
import time
from kafka import KafkaProducer

Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def clean_row(row):
    try:
        Clean and convert numeric fields
        row['user_id'] = int(float(row['user_id']))
        row['category_id'] = int(float(row['category_id']))
        row['price'] = float(row['price'])

        Fix event_time (keep only YYYY-MM-DD)
        row['event_time'] = row['event_time'].strip()[:10]

    except Exception as e:
        print(f"[WARN] Skipping row due to error: {e}")
        return None

    return row

Read and send CSV data
with open('2020-Feb.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        clean = clean_row(row)
        if clean is None:
            continue

        producer.send('shopping-events', value=clean)
        print(f"[Kafka] Sent: {json.dumps(clean, ensure_ascii=False)}")
        time.sleep(1)

Finalize
producer.flush()
producer.close()
print("[Kafka] Finished sending all records.")
