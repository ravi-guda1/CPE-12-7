import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
import pyspark
pyspark.sql.functions
import pandas as pd
from helpers.spark_helper import SparkHelper
from helpers.snowflake_helper import SnowflakeHelper
from helpers.local_helper import LocalHelper
from helpers.hive_helper import HiveHelper
import env
from pyspark.sql.functions import to_date
import pyspark.sql.functions as F
from pyspark.sql.functions import col,column
import json
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegralType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    BooleanType,
    MapType,
    TimestampType,
    TimestampNTZType,
    DayTimeIntervalType,
    StructType,
    DataType,
)
pyspark.sql.functions.column
from pyspark.sql.functions import avg
from pyspark.sql.functions import avg, to_date
from pyspark.sql.functions import avg, to_date, year, month
from pyspark.sql.functions import sum, to_date, year, month
from pyspark.sql.functions import sum, month, year
from pyspark.sql.functions import desc
from pyspark.sql.functions import count, to_date
from pyspark.sql.functions import asc


def load_csv():
    spark = SparkHelper.get_spark_session()
    cleansed_df = spark.read.csv(r"outputs\raw_layer.csv", header=True).coalesce(1)
    cleansed_df.dropDuplicates()



    #null_values = cleansed_df.isnull().sum()
    #null_values.show()
    # df.createOrReplaceTempView("my_temp_table")
    #order_count = cleansed_df.select('ProductKey').distinct().count()

    cleansed_df.printSchema()
    #convert type
    #conv=cleansed_df['ProductKey'] = cleansed_df['ProductKey'].astype(int)
    #print(type(conv))

    #cleansed_df.printSchema()
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