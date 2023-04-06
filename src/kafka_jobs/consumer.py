import sys
sys.path.append(".")

import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from config.config import config
from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(config['KAFKA']['KAFKA_ENDPOINT'], config['KAFKA']['KAFKA_ENDPOINT_PORT'])
KAFKA_TOPIC    = config['KAFKA']['KAFKA_TOPIC']

SNOWFLAKE_OPTIONS = {
    "sfURL" : config['SNOWFLAKE']['URL'],
    "sfAccount": config['SNOWFLAKE']['ACCOUNT'],
    "sfUser" : config['SNOWFLAKE']['USER'],
    "sfPassword" : config['SNOWFLAKE']['PASSWORD'],
    "sfDatabase" : config['SNOWFLAKE']['DATABASE'],
    "sfWarehouse" : config['SNOWFLAKE']['WAREHOUSE']
}

logger = Logger('Kafka-Consumer')

class Consumer:
  '''
    Consume data from Kafka's topic and store into Snowflake 

    Database: nyc_db
    Schema: nyc_lake
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
                  StructField("VendorID", IntegerType(), True),
                  StructField("tpep_pickup_datetime", StringType(), True),
                  StructField("tpep_dropoff_datetime", StringType(), True),
                  StructField("passenger_count", DoubleType(), True),
                  StructField("trip_distance", DoubleType(), True),
                  StructField("RatecodeID", DoubleType(), True),
                  StructField("store_and_fwd_flag", StringType(), True),
                  StructField("PULocationID", IntegerType(), True),
                  StructField("DOLocationID", IntegerType(), True),
                  StructField("payment_type", LongType(), True),
                  StructField("fare_amount", DoubleType(), True),
                  StructField("extra", DoubleType(), True),
                  StructField("mta_tax", DoubleType(), True),
                  StructField("tip_amount", DoubleType(), True),
                  StructField("tolls_amount", DoubleType(), True),
                  StructField("improvement_surcharge", DoubleType(), True),
                  StructField("total_amount", DoubleType(), True),
                  StructField("congestion_surcharge", DoubleType(), True),
                  StructField("airport_fee", DoubleType(), True)
          ])

    try:
      records = batch_df.count()

      parse_df = batch_df.rdd \
                          .map(lambda x: Consumer.fill_na(json.loads(x.value))) \
                          .toDF(schema)
      parse_df = parse_df \
                    .withColumn("created_at", lit(datetime.now())) \
                    .withColumnRenamed("VendorID","vendor_id") \
                    .withColumnRenamed("RatecodeID","ratecode_id") \
                    .withColumnRenamed("PULocationID","pu_location_id") \
                    .withColumnRenamed("DOLocationID","do_location_id")

      parse_df \
          .write \
          .format("snowflake") \
          .options(**SNOWFLAKE_OPTIONS) \
          .option("sfSchema", "nyc_lake") \
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
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "PULocationID", "DOLocationID", 
                "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                "total_amount", "ratecode_id", "store_and_fwd_flag", "payment_type", "passenger_count", "congestion_surcharge", "airport_fee"
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