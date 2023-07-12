import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("Read JSON from HDFS") \
    .getOrCreate()


df1 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2017-210509-235702.json")
df1.printSchema()
#sales=df1.show()
df2 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2016-210509-235702.json")
df3 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2015-210509-235702.json")
df4 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksTerritories-210509-235702.json")
df5 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksReturns-210509-235702.json")
df6 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksProductSubcategories-210509-235702.json")
df7 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksProducts-210509-235702.json")
df7.printSchema()
#products=df7.show()
#products.printSchema()
df8 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksProductCategories-210509-235702.json")
df9 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksCustomers-210509-235702.json")
df9.printSchema()
#customers=df9.show()
df10 = spark.read.option("multiline","true").json(r"E:\CPE-12-7-23\src\inputs\AdventureWorksCalendar-210509-235702.json")

Final_df= df1.join(df7,df7.ProductKey == df1.ProductKey).show()
cleansed_df = Final_df.withColumn("OrderDate", F.to_date("OrderDate", "M/d/yyyy")).show()




'''
# Calculate revenue per order
 t1= sales.withColumn("Revenue", expr("OrderQuantity * ProductPrice"))

# Calculate the average revenue per order
average_revenue_per_order = df.groupBy("OrderNumber").agg({"Revenue": "avg"})

# Print the average revenue per order
average_revenue_per_order.show()
'''