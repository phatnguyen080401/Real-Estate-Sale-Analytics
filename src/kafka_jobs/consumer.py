import sys
sys.path.append(".")

import json
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from config.config import config
from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(config['KAFKA']['KAFKA_ENDPOINT'], config['KAFKA']['KAFKA_ENDPOINT_PORT'])
KAFKA_TOPIC    = config['KAFKA']['KAFKA_TOPIC']

CLUSTER_ENDPOINT = "{0}:{1}".format(config['CASSANDRA']['CLUSTER_HOST'], config['CASSANDRA']['CLUSTER_PORT'])
CLUSTER_KEYSPACE = config['CASSANDRA']['CLUSTER_KEYSPACE']

logger = Logger('Kafka-Consumer')

class Consumer:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Streaming") \
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
            .option("kafka.group.id", "consumer_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_cassandra_lake(self, batch_df, batch_id):
    schema = StructType([
                  StructField("vendor_id", LongType(), True),
                  StructField("tpep_pickup_datetime", StringType(), True),
                  StructField("tpep_dropoff_datetime", StringType(), True),
                  StructField("passenger_count", DoubleType(), True),
                  StructField("trip_distance", DoubleType(), True),
                  StructField("ratecode_id", DoubleType(), True),
                  StructField("store_and_fwd_flag", StringType(), True),
                  StructField("pu_location_id", LongType(), True),
                  StructField("do_location_id", LongType(), True),
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

      uuid_generator = udf(lambda: str(uuid.uuid4()), StringType())

      parse_df = batch_df.rdd.map(lambda x: Consumer.parse(json.loads(x.value))).toDF(schema)
      parse_df = parse_df.withColumn("id", uuid_generator())

      parse_df \
          .write \
          .format("org.apache.spark.sql.cassandra") \
          .options(table="data_lake", keyspace=CLUSTER_KEYSPACE) \
          .mode("append") \
          .save()

      logger.info(f"Save to table: data_lake ({records} records)")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):   
    vendor_id = raw_data["VendorID"]
    tpep_pickup_datetime = raw_data["tpep_pickup_datetime"]
    tpep_dropoff_datetime = raw_data["tpep_dropoff_datetime"]
    trip_distance = raw_data["trip_distance"]
    pu_location_id = raw_data["PULocationID"]
    do_location_id = raw_data["DOLocationID"]
    payment_type = raw_data["payment_type"]
    fare_amount = raw_data["fare_amount"]
    extra = raw_data["extra"]
    mta_tax = raw_data["mta_tax"]
    tip_amount = raw_data["tip_amount"]
    tolls_amount = raw_data["tolls_amount"]
    improvement_surcharge = raw_data["improvement_surcharge"]
    total_amount = raw_data["total_amount"]

    if "ratecode_id" in raw_data:
      ratecode_id = raw_data["ratecode_id"]
    else:
      ratecode_id = None

    if "store_and_fwd_flag" in raw_data:
      store_and_fwd_flag = raw_data["store_and_fwd_flag"]
    else:
      store_and_fwd_flag = None

    if "payment_type" in raw_data:
      payment_type = raw_data["payment_type"]
    else:
      payment_type = None

    if "passenger_count" in raw_data:
      passenger_count = raw_data["passenger_count"]
    else:
      passenger_count = None

    if "congestion_surcharge" in raw_data:
      congestion_surcharge = raw_data["congestion_surcharge"]
    else:
      congestion_surcharge = None

    if "airport_fee" in raw_data:
      airport_fee = raw_data["airport_fee"]
    else:
      airport_fee = None

    data = {
              "vendor_id": vendor_id,
              "tpep_pickup_datetime": tpep_pickup_datetime,
              "tpep_dropoff_datetime": tpep_dropoff_datetime, 
              "passenger_count": passenger_count, 
              "trip_distance": trip_distance,
              "ratecode_id": ratecode_id,
              "store_and_fwd_flag": store_and_fwd_flag,
              "pu_location_id": pu_location_id,
              "do_location_id": do_location_id,
              "payment_type": payment_type, 
              "fare_amount": fare_amount, 
              "extra": extra,
              "mta_tax": mta_tax,
              "tip_amount": tip_amount,
              "tolls_amount": tolls_amount,
              "improvement_surcharge": improvement_surcharge,
              "total_amount": total_amount,
              "congestion_surcharge": congestion_surcharge,
              "airport_fee": airport_fee
            }

    return data

  def run(self):
    try:
      df = self.comsume_from_kafka()

      # tweets = df.select(('value')).alias("data").select("data.*")

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
  Consumer().run()