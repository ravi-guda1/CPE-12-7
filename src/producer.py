from kafka import  KafkaProducer
from json import dumps
import csv

topicname='retailupdate'
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda x:dumps(x).encode('utf-8'))
with open("E:\CPE-12-7-23\src\inputs\AdventureWorksSales2017-210509-235702.json",'r') as file:
 reader = csv.reader(file)
 for messages in reader:
  producer.send(topicname,messages)
  producer.flush()