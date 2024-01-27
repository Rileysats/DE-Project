from pyspark.sql import SparkSession

def setup_spark() -> SparkSession:
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark
