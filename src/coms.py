from kafka import KafkaConsumer
import sys

consumer = KafkaConsumer("retailupdate", bootstrap_servers=['localhost:9092'])
#hdfs = 'hdfs://localhost:9000//sunil/sunil.txt'
sys.stdout = open(r'C:\kafka\rama.csv', 'w')
for message in consumer:
      values = message.value
   # with hdfs.open(hdfs,'a') as wfile:
      print(values)
sys.stdout.close()