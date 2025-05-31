import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('2020-Feb.csv', 'r') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        producer.send('shopping-events', value=row)
        print(f"[Kafka] Trimis: {row}")
        time.sleep(0.1)  

        if i >= 100:
            break

producer.flush()
producer.close()
