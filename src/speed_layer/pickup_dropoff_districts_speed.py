from datetime import datetime
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

logger = Logger('Speed-Pickup-Dropoff-Districts')

class SpeedPickupDropoffDistricts: 
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Speed-Pickup-Dropoff-Districts") \
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
            .option("kafka.group.id", "pickup_dropoff_districts_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_cassandra(self, batch_df, batch_id):
    schema = StructType([
                  StructField("vendor_id", IntegerType(), True),
                  StructField("pu_location_id", LongType(), True),
                  StructField("do_location_id", LongType(), True)
          ])

    try:
        records = batch_df.count()
        
        parse_df = batch_df.rdd.map(lambda x: SpeedPickupDropoffDistricts.parse(json.loads(x.value))).toDF(schema)
        parse_df = parse_df.withColumn("created_at", lit(datetime.now()))

        parse_df \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="pickup_dropoff_districts_speed", keyspace=CLUSTER_KEYSPACE) \
            .mode("append") \
            .save()

        logger.info(f"Save to table: pickup_dropoff_districts_speed ({records} records)")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):
    vendor_id = raw_data["VendorID"]
    pu_location_id = raw_data["PULocationID"]
    do_location_id = raw_data["DOLocationID"]

    data = {
              'vendor_id': vendor_id,
              'pu_location_id': pu_location_id,
              'do_location_id': do_location_id
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
  SpeedPickupDropoffDistricts().run()