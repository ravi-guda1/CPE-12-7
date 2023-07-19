

import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json


from pyspark.sql import SparkSession

producer = KafkaProducer(bootstrap_servers=['192.168.56.1:9092'], #change ip here
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))




producer.send('sales', value={'surnasdasdame':'bunnyparasdasdmar1'})




df = pd.read_csv("E:\Products.csv")





df.head()





while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('sales', value=dict_stock)
    sleep(1)


# In[ ]:


producer.flush() #clear data from kafka server





