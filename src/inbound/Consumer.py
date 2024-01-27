from confluent_kafka import Consumer, TopicPartition
from KafkaConfig import *
import os 
from helpers import setup_spark
print('test')
# Initialize Spark session
spark = setup_spark()
print('test')
# Read data from Kafka topic using PySpark streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

spark.stop()

# # Extract value from Kafka message
# df = df.selectExpr("CAST(value AS STRING)")

# # Split the CSV values into separate columns
# df = df.withColumn("name", split("value", ",")[0].cast("string"))
# df = df.withColumn("age", split("value", ",")[1].cast("integer"))
# df = df.withColumn("occupation", split("value", ",")[2].cast("string"))

# # Display the streaming DataFrame
# query = df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Wait for the streaming query to terminate
# query.awaitTermination()
if __name__ == "__main__":
    print("AA")