from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaHDFSSave").getOrCreate()

# Set up Kafka parameters
kafka_bootstrap_servers = "192.168.56.1:9092"
kafka_topic_name = "cities"

# Define the output directory in HDFS
output_dir = "hdfs://localhost:9000/user/data.txt"

# Create a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .load()

# Write the streaming DataFrame to HDFS
query = df.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", output_dir) \
    .start()
print(query.value)

# Wait for the streaming query to finish
query.awaitTermination()
