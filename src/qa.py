import env
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, column
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

df10 = spark.read.option("multiline", "true").json(
        r"C:\Users\ravikumar.g\Downloads\sales_jsonset\AdventureWorksCalendar-210509-235702.json")

df.show()
#df10.write.format("csv").mode('overwrite').save("C:\demo")