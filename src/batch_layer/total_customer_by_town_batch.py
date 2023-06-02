import sys
sys.path.append(".")

import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from logger.logger import Logger

SNOWFLAKE_OPTIONS = {
    "sfURL" : os.getenv("SNOWFLAKE_URL"),
    "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser" : os.getenv("SNOWFLAKE_USER"),
    "sfPassword" : os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase" : os.getenv("SNOWFLAKE_DATABASE"),
    "sfWarehouse" : os.getenv("SNOWFLAKE_WAREHOUSE")
}

logger = Logger('Batch-Total-Customer-By-Town')

class BatchTotalCustomerByTown:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Total-Customer-By-Town") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
                    "net.snowflake:snowflake-jdbc:3.13.14," + 
                    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
                  ) \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def save_to_snowflake(self, batch_df):
    try:
      started_at = datetime.now()

      town_df = batch_df.select(col("town"), col("serial_number"))

      fill_null_df = town_df.na.fill("Unknown")

      total_customer_by_town_df = fill_null_df \
                                      .groupBy("town") \
                                      .count() \
                                      .toDF("town", "total_customer")

      time.sleep(300)

      ended_at = datetime.now()

      total_customer_by_town_df = total_customer_by_town_df \
                                            .withColumn("started_at", lit(started_at)) \
                                            .withColumn("ended_at", lit(ended_at))

      total_customer_by_town_df \
                      .write \
                      .format("snowflake") \
                      .options(**SNOWFLAKE_OPTIONS) \
                      .option("sfSchema", "sale_batch") \
                      .option("dbtable", "total_customer_by_town") \
                      .mode("overwrite") \
                      .save()

      df_rows = total_customer_by_town_df.count()
      logger.info(f"Save to table sale_batch.total_customer_by_town: {df_rows} rows")
    except Exception as e:
      logger.error(e)

  def run(self):
    try:
      df = self._spark \
                  .read \
                  .format("snowflake") \
                  .options(**SNOWFLAKE_OPTIONS) \
                  .option("sfSchema", "sale_lake") \
                  .option("dbtable", "data_lake") \
                  .load()

      logger.info(f"Read data from table sale_lake.data_lake")
      self.save_to_snowflake(df)
    except Exception as e:
      logger.error(e)
      
if __name__ == '__main__':
  BatchTotalCustomerByTown().run()