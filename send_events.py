import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def clean_row(row):
    try:
        row['user_id'] = int(float(row['user_id']))
        row['category_id'] = int(float(row['category_id']))
        row['price'] = float(row['price'])
        row['event_time'] = row['event_time'].strip()

    except Exception as e:
        return None

    return row

with open('tpd_dataset.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        clean = clean_row(row)
        if clean is None:
            continue

        producer.send('shopping-events', value=clean)
        print(f"[Kafka] Sent: {json.dumps(clean, ensure_ascii=False)}")
        time.sleep(0.20)

producer.flush()
producer.close()
