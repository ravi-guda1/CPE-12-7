import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))

from helpers.spark_helper import SparkHelper
from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper
from helpers.hive_helper import HiveHelper
import env
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F
from pyspark.sql.functions import col,column

def load_csv():
    spark = SparkHelper.get_spark_session()
    cleansed_df = spark.read.csv(r"outputs\raw_layer.csv", header=True).coalesce(1)
    cleansed_df.dropDuplicates()
    cleansed_df.printSchema()
    #cleansed_df.withColumn("CustomerKey",col("CustomerKey").cast("Integer")).show()
    #cleansed_df = cleansed_df.withColumn("StockDate", to_date(col("StockDate"), "dd/MM/yyyy"))
    cleansed_df.printSchema()
    return cleansed_df

def to_local(df):
    LocalHelper.save_df_internal(df, env.cleansed_layer_df_path)

def to_hive(df):
    HiveHelper().create_hive_database(SparkHelper.get_spark_session(), env.hive_db)
    HiveHelper().save_data_in_hive(df, env.hive_db, env.cleansed_layer_hive_table)
'''
#def to_snowflake(df):
    SnowflakeHelper().save_df_to_snowflake(df, env.cleansed_layer_sf_table)
'''
if __name__ == "__main__":
    df = load_csv()
    to_local(df)
    to_hive(df)
    #to_snowflake(df)