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

logger = Logger('Speed-Total-Sale-Amount-Ratio')

class SpeedTotalSaleAmountRatio:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Speed-Total-Sale-Amount-Ratio") \
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
            .option("kafka.group.id", "total_sale_amount_ratio_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_snowflake(self, batch_df, batch_id):
    schema = StructType([
                  StructField("sale_amount", DoubleType(), True),
                  StructField("sales_ratio", DoubleType(), True),
          ])

    try:
        parse_df = batch_df.rdd \
                            .map(lambda x: SpeedTotalSaleAmountRatio.parse(json.loads(x.value))) \
                            .toDF(schema)
        
        drop_null_row_df = parse_df.na.drop()

        total_customer = drop_null_row_df.count()
        
        total_sale_amount_ratio_df = drop_null_row_df \
                                          .agg({
                                                'sale_amount': 'sum',
                                                'sales_ratio': 'sum'
                                                }) \
                                          .toDF("total_sale_ratio", "total_sale_amount") \
                                          .withColumn("total_customer", lit(total_customer)) \
                                          .withColumn("created_at", lit(datetime.now())) 

        total_sale_amount_ratio_df \
            .write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("sfSchema", "sale_speed") \
            .option("dbtable", "total_sale_amount_ratio") \
            .mode("append") \
            .save()

        total_sale_ratio = total_sale_amount_ratio_df.collect()[0][0]
        total_sale_amount = total_sale_amount_ratio_df.collect()[0][1]
    
        logger.info(f"Save to table sale_speed.total_sale_amount_ratio ({total_sale_amount}, {total_sale_ratio}, {total_customer})")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):
    data = {}
    columns = ["sale_amount", "sales_ratio"]

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
  SpeedTotalSaleAmountRatio().run()