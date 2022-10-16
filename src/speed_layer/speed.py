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

  def save_to_cassandra_lake(self, batch_df, batch_id):
    schema = StructType([
                         StructField('user_id', LongType(), True),
                         StructField('user_location', StringType(), True),
                         StructField('user_name', StringType(), True),
                         StructField('hashtags', ArrayType(StringType()), True),
                         StructField('tweet_text', StringType(), True),
                         StructField('created_at', StringType(), True),
                        ])

    try:
        records = batch_df.count()
        parse_df = batch_df.rdd.map(lambda x: Speed.parse(json.loads(x.value))).toDF(schema)

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
    user_id = raw_data["user"]["id"]
    user_name = raw_data["user"]["name"]
    user_location = raw_data["user"]["location"]

    if raw_data["truncated"] == True:
      tweet_text = raw_data["extended_tweet"]["full_text"]
      hashtags = [hashtag["text"] for hashtag in raw_data["extended_tweet"]["entities"]["hashtags"]]
    else:
      tweet_text = raw_data["text"]
      hashtags = [hashtag["text"] for hashtag in raw_data["entities"]["hashtags"]]

    created_at = raw_data["created_at"]

    data = {
              'user_id': user_id, 
              'user_name': user_name, 
              'user_location': user_location,
              'hashtags': hashtags, 
              'tweet_text': tweet_text, 
              'created_at': created_at 
            }

    return data

  def run(self):
    try:
      df = self.comsume_from_kafka()

      stream = df \
            .writeStream \
            .trigger(processingTime='5 seconds') \
            .foreachBatch(self.save_to_cassandra_lake) \
            .outputMode("append") \
            .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  Speed().run()