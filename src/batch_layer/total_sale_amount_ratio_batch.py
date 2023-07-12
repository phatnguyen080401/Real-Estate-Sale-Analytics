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

logger = Logger('Batch-Total-Sale-Amount-Ratio')

class BatchTotalSaleAmountRatio:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Total-Sale-Amount-Ratio") \
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

      df = batch_df.select(col("sale_amount"), col("sales_ratio"))

      drop_null_row_df = df.na.drop()

      total_customer = drop_null_row_df.count()

      total_sale_amount_ratio_df = drop_null_row_df \
                                        .agg({
                                              'sale_amount': 'sum',
                                              'sales_ratio': 'sum'
                                              }) \
                                        .toDF("total_sale_ratio", "total_sale_amount") \
                                        .withColumn("total_customer", lit(total_customer)) 

      time.sleep(300)

      ended_at = datetime.now()

      total_sale_amount_ratio_df = total_sale_amount_ratio_df \
                                            .withColumn("started_at", lit(started_at)) \
                                            .withColumn("ended_at", lit(ended_at))

      total_sale_amount_ratio_df \
                      .write \
                      .format("snowflake") \
                      .options(**SNOWFLAKE_OPTIONS) \
                      .option("sfSchema", "sale_batch") \
                      .option("dbtable", "total_sale_amount_ratio") \
                      .mode("append") \
                      .save()

      total_sale_ratio = total_sale_amount_ratio_df.collect()[0][0]
      total_sale_amount = total_sale_amount_ratio_df.collect()[0][1]

      logger.info(f"Save to table sale_batch.total_sale_amount_ratio ({total_sale_amount}, {total_sale_ratio}, {total_customer})")
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
  BatchTotalSaleAmountRatio().run()