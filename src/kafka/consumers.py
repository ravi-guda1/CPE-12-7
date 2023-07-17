# from kafka import KafkaConsumer
import sys
#
# consumer = KafkaConsumer("Adventure-customers", bootstrap_servers=['localhost:9092'])
# sys.stdout = open(r'D:\CPE_project\salesdataset\test.csv', 'w')
# for message in consumer:
#       values = message.value
#       print(values)
# sys.stdout.close()

#
# from kafka import KafkaConsumer
# import pydoop.hdfs as hdfs
# consumer = KafkaConsumer('testTopic',bootstrap_servers=['localhost:9092'])
# hdfs_path = 'hdfs://localhost:9000/StockDatapydoop/stock_file.txt'
#
#
# for message in consumer:
#    values = message.value.decode('utf-8')
#    with hdfs.open(hdfs_path, 'at') as f:
#        print(message.value)
#        f.write(f"{values}\n")



from kafka import KafkaConsumer
from pyspark.sql import SparkSession

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "cities"

# HDFS configuration
hdfs_output_path = "hdfs://localhost:9000/output"

# Create Kafka consumer
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers)

# Consume and process Kafka messages
spark = SparkSession.builder.appName("Kafka to HDFS").getOrCreate()
for message in consumer:
    # Process the consumed message as per your requirements
    record = message.value.decode('utf-8')
    # Process the record using Spark or any other processing logic

    # Write processed data to HDFS
    # Example: Writing the record to HDFS as JSON
    df = spark.createDataFrame([record], "string")
    df.write.mode("append").json(hdfs_output_path)

# Close the consumer
consumer.close()
