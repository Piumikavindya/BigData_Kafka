from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader
import json
import io
import random
import time

schema = json.load(open("order.avsc"))

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(["orders"])

dlq_producer = Producer({'bootstrap.servers': 'localhost:9092'})

total_price = 0
count = 0

MAX_RETRIES = 3

def deserialize_avro(raw_bytes):
    buffer = io.BytesIO(raw_bytes)
    return schemaless_reader(buffer, schema)

def process_message(order):
    # RANDOM FAILURE for testing retry logic
    if random.random() < 0.2:
        raise Exception("Temporary failure!")

    global total_price, count
    total_price += order["price"]
    count += 1

    avg_price = total_price / count
    print(f"Processed: {order} | Running Avg: {avg_price:.2f}")

def send_to_dlq(order, error):
    dlq_producer.produce("orders_dlq", value=json.dumps({
        "order": order,
        "error": str(error),
        "time": time.time()
    }).encode("utf-8"))
    dlq_producer.flush()
    print("Moved to DLQ:", order)

print("Consumer started...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error:", msg.error())
        continue

    try:
        order = deserialize_avro(msg.value())
    except Exception as e:
        print("Failed Avro Deserialization → DLQ")
        send_to_dlq(msg.value(), e)
        continue

    retry = 0
    while retry < MAX_RETRIES:
        try:
            process_message(order)
            break
        except Exception as e:
            retry += 1
            print(f"Retry {retry} for message {order}")
            time.sleep(1)

            if retry == MAX_RETRIES:
                print("Sending to DLQ after max retries.")
                send_to_dlq(order, e)

consumer.close()
