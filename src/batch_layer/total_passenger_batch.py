import sys
sys.path.append(".")

import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from logger.logger import Logger

SNOWFLAKE_OPTIONS = {
    "sfURL" : config['SNOWFLAKE']['URL'],
    "sfAccount": config['SNOWFLAKE']['ACCOUNT'],
    "sfUser" : config['SNOWFLAKE']['USER'],
    "sfPassword" : config['SNOWFLAKE']['PASSWORD'],
    "sfDatabase" : config['SNOWFLAKE']['DATABASE'],
    "sfWarehouse" : config['SNOWFLAKE']['WAREHOUSE']
}

logger = Logger('Batch-Total-Passenger')

class BatchTotalPassenger:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Total-Passenger") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
                    "net.snowflake:snowflake-jdbc:3.13.14," + 
                    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
                  ) \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def save_to_snowflake(self, batch_df):
    try:
      total_rides = batch_df.count()

      drop_null_row_df = batch_df.na.drop()

      passenger_df = drop_null_row_df \
                            .select(col("passenger_count")) \
                            .agg({'passenger_count': 'sum'}) \
                            .toDF("total_passenger")

      total_passenger_df = passenger_df \
                                    .withColumn("total_rides", lit(total_rides)) \
                                    .withColumn("started_at", lit(datetime.now())) \

      time.sleep(300)

      total_passenger_df = total_passenger_df.withColumn("ended_at", lit(datetime.now()))

      total_passenger_df \
                      .write \
                      .format("snowflake") \
                      .options(**SNOWFLAKE_OPTIONS) \
                      .option("sfSchema", "yellow_taxi_batch") \
                      .option("dbtable", "total_passenger") \
                      .mode("append") \
                      .save()

      total_passenger = total_passenger_df.collect()[0][0]
      logger.info(f"Save to table yellow_taxi_batch.total_passenger ({total_passenger}, {total_rides})")
    except Exception as e:
      logger.error(e)

  def run(self):
    try:
      df = self._spark \
                  .read \
                  .format("snowflake") \
                  .options(**SNOWFLAKE_OPTIONS) \
                  .option("sfSchema", "nyc_lake") \
                  .option("dbtable", "data_lake") \
                  .load()
      
      logger.info(f"Read data from table nyc_lake.data_lake")
      self.save_to_snowflake(df)
    except Exception as e:
      logger.error(e)
      
if __name__ == '__main__':
  BatchTotalPassenger().run()