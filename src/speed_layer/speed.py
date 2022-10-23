import sys
sys.path.append(".")

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(config['KAFKA']['KAFKA_ENDPOINT'], config['KAFKA']['KAFKA_ENDPOINT_PORT'])
KAFKA_TOPIC    = config['KAFKA']['KAFKA_TOPIC']

CLUSTER_ENDPOINT = "{0}:{1}".format(config['CASSANDRA']['CLUSTER_HOST'], config['CASSANDRA']['CLUSTER_PORT'])
CLUSTER_KEYSPACE = config['CASSANDRA']['CLUSTER_KEYSPACE']

logger = Logger('Speed-Layer')

class Speed:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Speed-Layer") \
            .config("spark.cassandra.connection.host", CLUSTER_ENDPOINT) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
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
            .option("kafka.group.id", "speed_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_cassandra(self, batch_df, batch_id):
    schema = StructType([
                  StructField("vendor_id", LongType(), True),
                  StructField("tpep_pickup_datetime", StringType(), True),
                  StructField("tpep_dropoff_datetime", StringType(), True),
                  StructField("passenger_count", DoubleType(), True),
                  StructField("trip_distance", DoubleType(), True),
                  StructField("pu_location_id", LongType(), True),
                  StructField("do_location_id", LongType(), True),
                  StructField("fare_amount", DoubleType(), True),
                  StructField("tip_amount", DoubleType(), True),
                  StructField("total_amount", DoubleType(), True),
          ])

    try:
        records = batch_df.count()
        
        parse_df = batch_df.rdd.map(lambda x: Speed.parse(json.loads(x.value))).toDF(schema)
        parse_df = parse_df \
                  .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")) \
                  .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))

        parse_df \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="speed_layer", keyspace=CLUSTER_KEYSPACE) \
            .mode("append") \
            .save()

        logger.info(f"Save to table: speed_layer ({records} records)")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):
    vendor_id = raw_data["VendorID"]
    tpep_pickup_datetime = raw_data["tpep_pickup_datetime"]
    tpep_dropoff_datetime = raw_data["tpep_dropoff_datetime"]
    passenger_count = raw_data["passenger_count"]
    trip_distance = raw_data["trip_distance"]
    pu_location_id = raw_data["PULocationID"]
    do_location_id = raw_data["DOLocationID"]
    fare_amount = raw_data["fare_amount"]
    tip_amount = raw_data["tip_amount"]
    total_amount = raw_data["total_amount"]

    data = {
              'vendor_id': vendor_id,
              'tpep_pickup_datetime': tpep_pickup_datetime,
              'tpep_dropoff_datetime': tpep_dropoff_datetime, 
              'passenger_count': passenger_count, 
              'trip_distance': trip_distance,
              'pu_location_id': pu_location_id,
              'do_location_id': do_location_id,
              'fare_amount': fare_amount, 
              'tip_amount': tip_amount,
              'total_amount': total_amount
            }

    return data

  def run(self):
    try:
      df = self.comsume_from_kafka()

      stream = df \
            .writeStream \
            .foreachBatch(self.save_to_cassandra) \
            .outputMode("append") \
            .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  Speed().run()