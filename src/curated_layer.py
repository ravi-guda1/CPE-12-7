import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
from helpers.spark_helper import SparkHelper
from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper
from helpers.hive_helper import HiveHelper
from pyspark.sql.functions import count
import pyspark.sql.functions as F
import env
from pyspark.sql.window import Window

from pyspark.sql.functions import avg
from pyspark.sql.functions import avg, to_date
from pyspark.sql.functions import avg, to_date, year, month
from pyspark.sql.functions import sum, to_date, year, month
from pyspark.sql.functions import sum, month, year
from pyspark.sql.functions import desc
from pyspark.sql.functions import count, to_date
from pyspark.sql.functions import asc
from pyspark.sql.functions import max

def load_csv():
    spark = SparkHelper.get_spark_session()
    curated_df = spark.read.csv(env.cleansed_layer_df_path, header=True).coalesce(1)
    curated_df.printSchema()
    #curated_df.select("ProductKey").distinct().count()
    curated_df.createOrReplaceTempView("sales")
    spark.sql("select * from sales")
    count= spark.sql("SELECT COUNT(*) AS order_count FROM sales").show()
    #averageRevenueDF = spark.sql("SELECT AVG(ProductCost) AS average_revenue FROM sales")
    #averageRevenueDF.show()
    #revenuePerMonthPerYearDF = spark.sql(
    #    "SELECT year(date) AS year, month(date) AS month, SUM(ProductCost) AS total_revenue FROM sales GROUP BY year, month").show()
    #revenuePerMonthPerYearDF = spark.sql(
     #   """SELECT year(date) AS year,month(date) AS month,sum(ProductPrice) AS total_revenue FROM sales GROUP BY year,month ORDER BY year,month """).show()
    #highestPricedProductDF = spark.sql(
      #  """ SELECT * FROM sales WHERE ProductPrice = (SELECT MAX(ProductPrice) FROM sales) """)
    #highestPricedProductDF.show()
    #orderCountByDateDF = spark.sql(
      #  """ SELECT OrderDate ,COUNT(*) AS order_count FROM sales GROUP BY OrderDate ORDER BY OrderDate """).show()
    #sortedProductsByCategoryDF = spark.sql("""SELECT * FROM sales ORDER BY ProductSubcategoryKey ASC """)
    #sortedProductsByCategoryDF.show()

    '''
    curated_df = curated_df.withColumn('DiscountAmount', F.when(F.col('DiscountAmount') == 0, "N").otherwise("Y")) \
                .withColumn("salesprice-freight-taxes-promotion", F.col("SalesAmount") - ( F.col("TaxAmount") + F.col("Freight") )) \
                .withColumnRenamed('DiscountAmount', 'Discount_present')
    '''
    return curated_df
'''
def load_agg_region_csv():
    spark = SparkHelper.get_spark_session()
    curated_df = spark.read.csv(env.curated_layer_df_path, header=True).coalesce(1)
    curated_df = curated_df.groupBy("Category").agg(F.sum("OrderQuantity").alias("OrderQuantity"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(F.col("OrderQuantity").desc())
    return curated_df

def load_agg_category_csv():
    spark = SparkHelper.get_spark_session()
    curated_df = spark.read.csv(env.curated_layer_df_path, header=True).coalesce(1)
    curated_df = curated_df.groupBy("Category", "Subcategory").agg(F.sum("OrderQuantity").alias("OrderQuantity"), F.sum("SalesAmount").alias("SalesAmount")).orderBy(F.col("OrderQuantity").asc())
    w2 = Window.partitionBy("Category").orderBy(F.col("OrderQuantity").desc())
    curated_df= curated_df.withColumn("row", F.row_number().over(w2)) \
        .filter(F.col("row") <11).drop("row")
    return curated_df
'''
def to_local(df):
    LocalHelper.save_df_internal(df, env.curated_layer_df_path)

def region_to_local(df):
    LocalHelper.save_df_internal(df, env.agg_region_df_path)

def category_to_local(df):
    LocalHelper.save_df_internal(df, env.agg_category_df_path)
'''
def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, env.curated_layer_sf_table)


def region_to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, env.agg_region_sf_table)

def category_to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, env.agg_category_sf_table)
'''
def to_hive(df, table):
    HiveHelper().create_hive_database(SparkHelper.get_spark_session(), env.hive_db)
    HiveHelper().save_data_in_hive(df, env.hive_db, table)

if __name__ == "__main__":
    df = load_csv()
    to_local(df)
    to_hive(df, env.curated_layer_hive_table)

    #to_snowflake(df)

    #df = load_agg_region_csv()
    region_to_local(df)
    to_hive(df, env.agg_region_hive_table)
    #region_to_snowflake(df)

    #df = load_agg_category_csv()
    category_to_local(df)
    to_hive(df, env.agg_category_hive_table)
    #category_to_snowflake(df)
    

