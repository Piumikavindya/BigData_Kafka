from confluent_kafka import Producer
from fastavro import schemaless_writer
import json
import random
import time
import io

# Load Avro schema
schema = json.load(open("order.avsc"))

producer = Producer({'bootstrap.servers': 'localhost:9092'})

def serialize_avro(data):
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, data)
    return buffer.getvalue()

products = ["Item1", "Item2", "Item3"]

while True:
    order = {
        "orderId": str(random.randint(1000, 9999)),
        "product": random.choice(products),
        "price": round(random.uniform(10, 500), 2)
    }

    avro_bytes = serialize_avro(order)

    producer.produce("orders", value=avro_bytes)
    producer.flush()

    print("Sent:", order)

    time.sleep(1)
