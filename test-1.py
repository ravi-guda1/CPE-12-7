from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Set the Kafka consumer configuration
kafka_conf = {
    "kafka.bootstrap.servers": "192.168.56.1:9092",
    "subscribe": "sales",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_conf) \
    .load()

# Extract the value column (message) as a string
df = df.selectExpr("CAST(value AS STRING)")

# Save the streaming data into a destination
query = df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("path", "hdfs://localhost:9000/user") \
    .start()

# Wait for the query to terminate
query.awaitTermination()