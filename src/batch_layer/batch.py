import sys
sys.path.append(".")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from logger.logger import Logger

CLUSTER_ENDPOINT = "{0}:{1}".format(config['CASSANDRA']['CLUSTER_HOST'], config['CASSANDRA']['CLUSTER_PORT'])
CLUSTER_KEYSPACE = config['CASSANDRA']['CLUSTER_KEYSPACE']

logger = Logger('Batch-Layer')

class Batch:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Layer") \
            .config("spark.cassandra.connection.host", CLUSTER_ENDPOINT) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def save_to_cassandra(self, tweet_df):
    columns = ['user_id', 'user_name', 'tweet_text', 'created_at']

    try:
        records = tweet_df.count()

        tweet_df2 = tweet_df.select([col(column_name) for column_name in columns])

        tweet_df2 \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="batch_layer", keyspace=CLUSTER_KEYSPACE) \
            .mode("append") \
            .save()

        logger.info(f"Save to table: batch_layer ({records} records)")
    except Exception as e:
      logger.error(e)

  def run(self):
    try:
      df = self._spark \
                  .read \
                  .format("org.apache.spark.sql.cassandra") \
                  .options(table="twitter_lake", keyspace=CLUSTER_KEYSPACE) \
                  .load()

      self.save_to_cassandra(df)
    except Exception as e:
      logger.error(e)
      
if __name__ == '__main__':
  Batch().run()