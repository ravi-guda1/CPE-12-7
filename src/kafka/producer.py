'''
# from kafka import  KafkaProducer
# from json import dumps
# import csv
#
# topicname='Adventure-customers'
# producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:dumps(x).encode('utf-8'))
# with open("D:\CPE_project\salesdataset\AdventureWorksCustomers.csv",'r') as file:
#  reader = csv.reader(file)
#  for messages in reader:
#   producer.send(topicname,messages)
#   producer.flush()
'''
from kafka import KafkaProducer
import csv
import json

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "TEST_DATA"

# CSV file path
csv_file_path = "C:\\Users\\ravikumar.g\\Downloads\\salesdataset\\AdventureWorksCustomers-210509-235702.csv"

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# Read CSV file and send data to Kafka
with open(csv_file_path, 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        message = json.dumps(row).encode('utf-8')
        producer.send(kafka_topic, value=message)

# Close the producer
producer.close()

