import sys
sys.path.append(".")

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic

from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(os.getenv("KAFKA_ENDPOINT"), os.getenv("KAFKA_ENDPOINT_PORT"))
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC")

logger = Logger("Kafka-Producer")

class Producer:
  '''
    Ingest data from folder data containing mulptiple parquet file by Spark Streaming and store into Kafka's topic 

    Folder: data
  '''


  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Producer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .config('spark.sql.session.timeZone', 'UTC') \
            .getOrCreate()
    self._spark.sparkContext.setLogLevel("ERROR")
    self.topic = []

  # def create_topic(self):
  #   try:
  #     admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_ENDPOINT)

  #     self.topic.append(NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1))
  #     admin_client.create_topics(new_topics=self.topic, validate_only=False)

  #     logger.info(f"Create topic: {KAFKA_TOPIC}")
  #   except TopicAlreadyExistsError as e:
  #     logger.error(e)

  def read_file(self):
    schema = StructType([
                  StructField("serial_number", LongType(), True),
                  StructField("list_year", LongType(), True),
                  StructField("date_recorded", DateType(), True),
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
      df = self._spark \
            .readStream \
            .schema(schema) \
            .parquet(f"./data/")


      logger.info(f"Reading file...")
    except Exception as e:
      logger.error(e)

    return df

  def produce(self):
    # self.create_topic()

    df = self.read_file()
    cols = df.columns

    try:
      stream = df \
              .select(to_json(struct(*cols)).alias("value")) \
              .selectExpr("CAST(value AS STRING)") \
              .writeStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", KAFKA_ENDPOINT) \
              .option("topic", KAFKA_TOPIC) \
              .option("checkpointLocation", "./checkpoint") \
              .trigger(processingTime='5 seconds') \
              .outputMode("append") \
              .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  producer = Producer()
  producer.produce()