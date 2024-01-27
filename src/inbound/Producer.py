import csv
import os
import json

from confluent_kafka import Producer
from KafkaConfig import *

# Kafka producer configuration
producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "queue.buffering.max.messages": MAX_MESSAGES
}

# Create Kafka producer
producer = Producer(producer_config)

# Produce data to Kafka topic
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

current_directory = os.path.dirname(os.path.abspath(__file__))
PATH = f"{current_directory}/data/{DATA_FILE}.csv" # https://www.kaggle.com/datasets/shivamb/bank-customer-segmentation?resource=download

# Read data from CSV and produce to Kafka topic
with open(PATH, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    header = next(csv_reader)  # Skip header

    for row in csv_reader:
        # TransactionID,CustomerID,CustomerDOB,CustGender,CustLocation,CustAccountBalance,TransactionDate,TransactionTime,TransactionAmount (INR)
        row = [
            row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]
        ]

        # Define keys
        keys = [
            "CustomerID", "CustomerDOB", "CustGender", "CustLocation",
            "CustAccountBalance", "TransactionDate", "TransactionTime", "TransactionAmount"
        ]

        # Create a dictionary using zip
        data_dict = dict(zip(keys, row))

        # Convert dictionary to JSON string
        json_string = json.dumps(data_dict, indent=2)

        message = json_string
        producer.produce(KAFKA_TOPIC, key=row[0], value=message, callback=delivery_report)

# # Produce a sample message
# sample_message = 'Hello, Kafka!'

# # Produce the message to the Kafka topic
# producer.produce(kafka_topic, value=sample_message, callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()
