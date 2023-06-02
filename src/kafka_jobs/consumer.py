import sys
sys.path.append(".")

import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

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

logger = Logger('Kafka-Consumer')

class Consumer:
  '''
    Consume data from Kafka's topic and store into Snowflake 

    Database: sale_db
    Schema: sale_lake
    Table: data_lake
  '''

  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Consumer") \
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
            .option("kafka.group.id", "consumer_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_data_lake(self, batch_df, batch_id):
    schema = StructType([
                  StructField("serial_number", LongType(), True),
                  StructField("list_year", LongType(), True),
                  StructField("date_recorded", StringType(), True),
                  StructField("town", StringType(), True),
                  StructField("address", StringType(), True),
                  StructField("assessed_value", DoubleType(), True),
                  StructField("sale_amount", DoubleType(), True),
                  StructField("sales_ratio", DoubleType(), True),
                  StructField("property_type", StringType(), True),
                  StructField("residential_type", StringType(), True),
                  StructField("non_use_code", StringType(), True),
                  StructField("assessor_remarks", StringType(), True),
                  StructField("opm_remarks", StringType(), True),
                  StructField("location", StringType(), True),
          ])

    try:
      records = batch_df.count()

      parse_df = batch_df.rdd \
                          .map(lambda x: Consumer.fill_na(json.loads(x.value))) \
                          .toDF(schema)
      
      parse_df = parse_df \
                    .withColumn("date_recorded", to_date(col("date_recorded"))) \
                    .withColumn("created_at", lit(datetime.now()))

      parse_df \
          .write \
          .format("snowflake") \
          .options(**SNOWFLAKE_OPTIONS) \
          .option("sfSchema", "sale_lake") \
          .option("dbtable", "data_lake") \
          .options(header=True) \
          .mode("append") \
          .save()

      logger.info(f"Save to table: data_lake ({records} records)")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def fill_na(raw_data):
    '''
      Fill null to missing column
    '''

    columns = [
                "serial_number", "list_year", "date_recorded", "town", "address", "assessed_value", "sale_amount",
                "sales_ratio", "property_type", "residential_type", "non_use_code", "assessor_remarks", "opm_remarks",
                "location"
              ]

    for column in columns:
      if column not in raw_data:
        raw_data[column] = None

    return raw_data
  
  def run(self):
    try:
      df = self.comsume_from_kafka()

      stream = df \
            .writeStream \
            .trigger(processingTime='5 seconds') \
            .foreachBatch(self.save_to_data_lake) \
            .outputMode("append") \
            .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  Consumer().run()