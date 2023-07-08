import sys
sys.path.append(".")

import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime

from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(os.getenv("KAFKA_ENDPOINT"), os.getenv("KAFKA_ENDPOINT_PORT"))
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC")

SNOWFLAKE_OPTIONS = {
    "sfURL" : os.getenv("SNOWFLAKE_URL"),
    "sfAccount": os.getenv("SNOWFLAKE_ACCOUNT"),
    "sfUser" : os.getenv("SNOWFLAKE_USER"),
    "sfPassword" : os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase" : os.getenv("SNOWFLAKE_DATABASE"),
    "sfWarehouse" : os.getenv("SNOWFLAKE_WAREHOUSE")
}

logger = Logger('Speed-Total-Customer-By-Town')

class SpeedTotalCustomerByTown:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Speed-Total-Customer-By-Town") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
                    "net.snowflake:snowflake-jdbc:3.13.14," + 
                    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
                  ) \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def comsume_from_kafka(self):
    try:
      df = self._spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_ENDPOINT) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("kafka.group.id", "total_customer_by_town_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_snowflake(self, batch_df, batch_id):
    schema = StructType([
                  StructField("town", StringType(), True),
                  StructField("serial_number", LongType(), True)
          ])

    try:        
        parse_df = batch_df.rdd \
                            .map(lambda x: SpeedTotalCustomerByTown.parse(json.loads(x.value))) \
                            .toDF(schema)
        
        drop_null_row_df = parse_df.na.fill("Unknown")
        
        total_customer_by_town_df = drop_null_row_df \
                                            .groupBy("town") \
                                            .count() \
                                            .toDF("town", "total_customer") \
                                            .withColumn("created_at", lit(datetime.now())) \

        total_customer_by_town_df \
                .write \
                .format("snowflake") \
                .options(**SNOWFLAKE_OPTIONS) \
                .option("sfSchema", "sale_speed") \
                .option("dbtable", "total_customer_by_town") \
                .mode("append") \
                .save()

        df_rows = total_customer_by_town_df.count()
        logger.info(f"Save to table sale_speed.total_customer_by_town: {df_rows} rows")    
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):
    data = {}
    columns = ["town", "serial_number"]

    for column in columns:
      if column in raw_data:
        data[column] = raw_data[column]
      else:
        data[column] = None

    return data

  def run(self):
    try:
      df = self.comsume_from_kafka()

      stream = df \
            .writeStream \
            .foreachBatch(self.save_to_snowflake) \
            .outputMode("append") \
            .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  SpeedTotalCustomerByTown().run()