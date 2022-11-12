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

logger = Logger('Batch-Total-Passenger')

class BatchTotalPassenger:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Total-Passenger") \
            .config("spark.cassandra.connection.host", CLUSTER_ENDPOINT) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def save_to_cassandra(self, batch_df):
    try:
      total_rides = batch_df.count()

      passenger_df = batch_df \
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
                      .format("org.apache.spark.sql.cassandra") \
                      .options(table="total_passenger_batch", keyspace=CLUSTER_KEYSPACE) \
                      .mode("append") \
                      .save()

      total_passenger = total_passenger_df.collect()[0][0]
      logger.info(f"Save to total_passenger_batch ({total_passenger}, {total_rides})")
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
  BatchTotalPassenger().run()