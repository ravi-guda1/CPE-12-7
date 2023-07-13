from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Order Analysis") \
    .getOrCreate()

# Assuming you have a DataFrame named "df" with the provided columns
df = spark.createDataFrame([
    ("23791", "1/1/2017", "2", "SO61285", "2", "529", "12/13/2003", "1"),
    # More rows...
], ["CustomerKey", "OrderDate", "OrderLineItem", "OrderNumber", "OrderQuantity", "ProductKey", "StockDate", "TerritoryKey"])

# Count the number of orders
orderCount = df.select("OrderNumber").distinct().count()

print("Number of Orders Placed:", orderCount)
