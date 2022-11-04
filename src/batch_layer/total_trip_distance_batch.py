import sys
sys.path.append(".")

import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from logger.logger import Logger

CLUSTER_ENDPOINT = "{0}:{1}".format(config['CASSANDRA']['CLUSTER_HOST'], config['CASSANDRA']['CLUSTER_PORT'])
CLUSTER_KEYSPACE = config['CASSANDRA']['CLUSTER_KEYSPACE']

logger = Logger('Batch-Total-Trip-Distance')

class BatchTotalTripDistance:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Total-Trip-Distance") \
            .config("spark.cassandra.connection.host", CLUSTER_ENDPOINT) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def save_to_cassandra(self, batch_df):
    try:
      total_rides = batch_df.count()

      trip_distance_df = batch_df \
                            .select(col("trip_distance")) \
                            .agg({'trip_distance': 'sum'}) \
                            .toDF("total_trip_distance")

      total_trip_distance_df = trip_distance_df \
                                    .withColumn("total_rides", lit(total_rides)) \
                                    .withColumn("started_at", lit(datetime.now())) \

      time.sleep(300)

      total_trip_distance_df = total_trip_distance_df.withColumn("ended_at", lit(datetime.now()))

      total_trip_distance_df \
                      .write \
                      .format("org.apache.spark.sql.cassandra") \
                      .options(table="total_trip_distance_batch", keyspace=CLUSTER_KEYSPACE) \
                      .mode("append") \
                      .save()

      total_distance = total_trip_distance_df.collect()[0][0]
      logger.info(f"total_trip_distance_batch ({total_distance}, {total_rides})")
    except Exception as e:
      logger.error(e)

  def run(self):
    try:
      df = self._spark \
                  .read \
                  .format("org.apache.spark.sql.cassandra") \
                  .options(table="data_lake", keyspace=CLUSTER_KEYSPACE) \
                  .load()

      self.save_to_cassandra(df)
    except Exception as e:
      logger.error(e)
      
if __name__ == '__main__':
  BatchTotalTripDistance().run()