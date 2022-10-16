import sys
sys.path.append(".")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.config import config
from logger.logger import Logger

CLUSTER_ENDPOINT = "{0}:{1}".format(config['CASSANDRA']['CLUSTER_HOST'], config['CASSANDRA']['CLUSTER_PORT'])
CLUSTER_KEYSPACE = config['CASSANDRA']['CLUSTER_KEYSPACE']

logger = Logger("Serving-Layer")

class Serving:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Batch-Layer") \
            .config("spark.cassandra.connection.host", CLUSTER_ENDPOINT) \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0") \
            .getOrCreate()
    
    self._spark.sparkContext.setLogLevel("ERROR")

  def get_batch_data(self):
    try:
      batch_df = self._spark \
                    .read \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="batch_layer", keyspace=CLUSTER_KEYSPACE) \
                    .load()

      logger.info("Get data from batch layer")
    except Exception as e:
      logger.error(e)

    return batch_df

  def get_speed_data(self):
    try:
      speed_df = self._spark \
                    .read \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="speed_layer", keyspace=CLUSTER_KEYSPACE) \
                    .load()
    
      logger.info("Get data from speed layer")
    except Exception as e:
      logger.error(e)
    
    return speed_df

  def get_top_100_users_tweet_words(self):
    '''
      Return at most top 100 mutually used words in tweets
    '''
    
    batch_df = self.get_batch_data()

    batch_df_to_pandas = batch_df.rdd.sortBy(lambda x: len(x['tweet_text']), ascending=False) \
                                            .toDF() \
                                            .to_pandas_on_spark()
    batch_df_to_pandas["word_count"] = batch_df_to_pandas["tweet_text"].map(lambda x: len(x))
    
    top_100_users_tweet_words = batch_df_to_pandas \
                                              .groupby("user_name") \
                                              .first() \
                                              .sort_values("word_count") \
                                              .head(100)

    return top_100_users_tweet_words

  def get_top_100_users_hashtags(self):
    '''
      Return at most top 100 mutually used hashtags in tweets
    '''
    
    speed_df = self.get_speed_data()

    speed_df_to_pandas = speed_df.rdd.sortBy(lambda x: len(x['hashtags']), ascending=False) \
                                         .toDF() \
                                         .to_pandas_on_spark()
    speed_df_to_pandas["hashtag_count"] = speed_df_to_pandas["hashtags"].map(lambda x: len(x))
    
    top_100_users_hashtags = speed_df_to_pandas \
                                              .groupby("user_name") \
                                              .first() \
                                              .sort_values("hashtag_count") \
                                              .head(100)

    return top_100_users_hashtags