


consumer = KafkaConsumer(
    'sales',
     bootstrap_servers=['192.168.56.1:9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))


# In[6]:


spark = SparkSession.builder.appName("Kafka to HDFS").getOrCreate()


# In[18]:


from pyspark.sql.types import StructType, StringType,StructField
from pyspark.sql.types import DoubleType, StringType, IntegerType, BooleanType


for c in consumer:
    print(c.value)



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

query.awaitTermination()






