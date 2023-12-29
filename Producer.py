from confluent_kafka import Producer
import time
import csv
import json

conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'btc-producer'
}

producer = Producer(conf)
topic = 'btc'
csv_file = 'BTCProducer.csv'

def produce_to_kafka(row):
    try:
        json_data = json.dumps({
            'Date': row['Date'],
            'Open': float(row['Open']),
            'High': float(row['High']),
            'Low': float(row['Low']),
            'Close': float(row['Close']),
            'Adj Close': float(row['Adj Close']),
            'Volume': int(row['Volume'])
        })

        producer.produce(topic, key=row['Date'], value=json_data)
        producer.flush()

        print(f"Sent data to Kafka: {json_data}")

    except Exception as e:
        print(f"Error producing data to Kafka: {e}")

with open(csv_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        produce_to_kafka(row)
        time.sleep(10)

producer.flush()
producer.close()
