import sys
sys.path.append(".")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic

from config.config import config
from logger.logger import Logger

KAFKA_TOPIC    = config['KAFKA']['KAFKA_TOPIC']
KAFKA_ENDPOINT = "{0}:{1}".format(config['KAFKA']['KAFKA_ENDPOINT'], config['KAFKA']['KAFKA_ENDPOINT_PORT'])

logger = Logger("Kafka-Producer")

class Producer:
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

  def create_topic(self):
    try:
      admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_ENDPOINT)

      self.topic.append(NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1))
      admin_client.create_topics(new_topics=self.topic, validate_only=False)

      logger.info(f"Create topic: {KAFKA_TOPIC}")
    except TopicAlreadyExistsError as e:
      logger.error(e)

  def read_file(self):
    schema = StructType([
                  StructField("VendorID", LongType(), True),
                  StructField("tpep_pickup_datetime", TimestampType(), True),
                  StructField("tpep_dropoff_datetime", TimestampType(), True),
                  StructField("passenger_count", DoubleType(), True),
                  StructField("trip_distance", DoubleType(), True),
                  StructField("RatecodeID", DoubleType(), True),
                  StructField("store_and_fwd_flag", StringType(), True),
                  StructField("PULocationID", LongType(), True),
                  StructField("DOLocationID", LongType(), True),
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
      df = self._spark \
            .readStream \
            .schema(schema) \
            .parquet(f"./data/")


      logger.info(f"Reading file...")
    except Exception as e:
      logger.error(e)

    return df

  def produce(self):
    self.create_topic()

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