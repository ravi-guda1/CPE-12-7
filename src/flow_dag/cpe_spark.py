import env
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, column
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg
from pyspark.sql.functions import avg, to_date
from pyspark.sql.functions import avg, to_date, year, month
from pyspark.sql.functions import sum, to_date, year, month
from pyspark.sql.functions import sum, month, year
from pyspark.sql.functions import desc
from pyspark.sql.functions import count, to_date
from pyspark.sql.functions import asc
'''
def load_data(df):
    spark = SparkSession.builder.appName("KafkaHDFSSave").getOrCreate()
    sales = spark.read.option("multiline", "true").json(
            r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2017-210509-235702.json")

    product = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksProducts-210509-235702.json")

    sales.write.format("json").mode('overwrite').save("hdfs://localhost:9000/datasets")
    product.write.format("json").mode('overwrite').save("hdfs://localhost:9000/datasets")


    data = product.join(sales, product.ProductKey == sales.ProductKey)
    data.printSchema()
    return data.show()

    #data.write.format("json").mode('overwrite').save("hdfs://localhost:9000/datasets")

'''
from pyspark.sql import SparkSession

def load_data():
    spark = SparkSession.builder.appName("KafkaHDFSSave").getOrCreate()

    sales = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2017-210509-235702.json"
    )

    product = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksProducts-210509-235702.json"
    )

    #sales.write.format("json").mode("overwrite").save("hdfs://localhost:9000/datasets/sales")
    #product.write.format("json").mode("overwrite").save("hdfs://localhost:9000/datasets/products")

    data = product.join(sales, product.ProductKey == sales.ProductKey)
    data.printSchema()
    return data

# Call the function to load and join the data
joined_data = load_data()

joined_data.show()

# The data has been written to HDFS and the joined DataFrame is available in `joined_data`
