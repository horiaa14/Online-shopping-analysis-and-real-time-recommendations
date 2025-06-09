import csv
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

mock_brands = [
    "BrandA", "BrandB", "BrandC", "BrandD", "BrandE"
]

mock_category_code = [
    "CategoryCodeA", "CategoryCodeB", "CategoryCodeC", "CategoryCodeD", "CategoryCodeE"
]

def clean_row(row):
    try:
        row['user_id'] = int(float(row['user_id']))
        row['category_id'] = int(float(row['category_id']))
        row['price'] = float(row['price'])
        row['event_time'] = row['event_time'].strip()[:10]

        if not row.get('brand') or row['brand'].strip() == '':
            row['brand'] = random.choice(mock_brands)
        
        if not row.get('category_code') or row['category_code'].strip() == '':
            row['category_code'] = random.choice(mock_category_code)

    except Exception as e:
        print(f"[WARN] Skipping row due to error: {e}")
        return None

    return row

with open('2020-Feb.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        clean = clean_row(row)
        if clean is None:
            continue

        producer.send('shopping-events', value=clean)
        print(f"[Kafka] Sent: {json.dumps(clean, ensure_ascii=False)}")
        time.sleep(1)

producer.flush()
producer.close()
print("[Kafka] Finished sending all records.")
