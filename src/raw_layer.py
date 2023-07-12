import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
from pyspark.sql import SparkSession
from helpers.spark_helper import SparkHelper
#from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper
from helpers.hive_helper import HiveHelper
import env
import json

def load_json():
    spark=SparkHelper.get_spark_session()
    df1 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2017-210509-235702.json")
    #df1.printSchema()
    # sales=df1.show()
    df2 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2016-210509-235702.json")
    df3 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksSales2015-210509-235702.json")
    df4 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksTerritories-210509-235702.json")
    df5 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksReturns-210509-235702.json")
    df6 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksProductSubcategories-210509-235702.json")
    df7 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksProducts-210509-235702.json")
    #df7.printSchema()
    # products=df7.show()
    # products.printSchema()
    df8 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksProductCategories-210509-235702.json")
    df9 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksCustomers-210509-235702.json")
    df9.printSchema()
    # customers=df9.show()
    df10 = spark.read.option("multiline", "true").json(
        r"E:\CPE-12-7-23\src\inputs\AdventureWorksCalendar-210509-235702.json")

    raw_df = df1.join(df7, df7.ProductKey == df1.ProductKey)
    # raw_df = raw_df.na.drop()
    return raw_df

def to_local(df):
    LocalHelper.save_df_internal(df, env.raw_layer_df_path)

def to_hive(df):
    HiveHelper().create_hive_database(SparkHelper.get_spark_session(), env.hive_db)
    HiveHelper().save_data_in_hive(df, env.hive_db, env.raw_layer_hive_table)
'''
def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, env.raw_layer_sf_table)
'''
if __name__=="__main__":
    df=load_json()
    to_local(df)
    to_hive(df)
   # to_snowflake(df)
