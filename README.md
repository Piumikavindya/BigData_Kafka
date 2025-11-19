# BigData_Kafka

## Kafka + Avro Orders Processing System

This project implements a simple real-time ingestion and processing pipeline using **Apache Kafka** with **Avro serialization**, including:
- Kafka Producer (Avro serialization)
- Kafka Consumer (Avro deserialization)
- Retry mechanism
- Dead Letter Queue (DLQ)
- Running average calculation
- Step-by-step instructions to set up and run the system

This README provides **complete end-to-end instructions** to install Kafka, create topics, set up Python, and run the full system.

---

## 🚀 1. Install Required Dependencies (Ubuntu)

sudo apt update
sudo apt upgrade -y
sudo apt install -y openjdk-17-jdk python3 python3-venv python3-pip wget

### Generate Cluster ID
cd ~/kafka
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID

### Start Kafka Server
bin/kafka-server-start.sh config/kraft/server.properties

## 🚀 2. Create Required Kafka Topics

cd ~/kafka
bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic orders_dlq --bootstrap-server localhost:9092

## 🚀 3. Run the System


python3 -m venv venv
source venv/bin/activate

python producer.py
python consumer.py

### View DLQ Messages

cd ~/kafka
bin/kafka-console-consumer.sh --topic orders_dlq --from-beginning --bootstrap-server localhost:9092

