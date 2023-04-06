import sys
sys.path.append(".")

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime

from config.config import config
from logger.logger import Logger

KAFKA_ENDPOINT = "{0}:{1}".format(config['KAFKA']['KAFKA_ENDPOINT'], config['KAFKA']['KAFKA_ENDPOINT_PORT'])
KAFKA_TOPIC    = config['KAFKA']['KAFKA_TOPIC']

SNOWFLAKE_OPTIONS = {
    "sfURL" : config['SNOWFLAKE']['URL'],
    "sfAccount": config['SNOWFLAKE']['ACCOUNT'],
    "sfUser" : config['SNOWFLAKE']['USER'],
    "sfPassword" : config['SNOWFLAKE']['PASSWORD'],
    "sfDatabase" : config['SNOWFLAKE']['DATABASE'],
    "sfWarehouse" : config['SNOWFLAKE']['WAREHOUSE']
}

logger = Logger('Speed-Total-Passenger')

class SpeedTotalPassenger:
  def __init__(self):
    self._spark = SparkSession \
            .builder \
            .master("local[*]") \
            .appName("Speed-Total-Passenger") \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0," +
                    "net.snowflake:snowflake-jdbc:3.13.14," + 
                    "net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
                  ) \
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
            .option("kafka.group.id", "passenger_group") \
            .load()

      df = df.selectExpr("CAST(value AS STRING)")

      logger.info(f"Consume topic: {KAFKA_TOPIC}")
    except Exception as e:
      logger.error(e)

    return df

  def save_to_snowflake(self, batch_df, batch_id):
    schema = StructType([
                  StructField("passenger_count", DoubleType(), True)
          ])

    try:
        total_rides = batch_df.count()
        
        parse_df = batch_df.rdd \
                            .map(lambda x: SpeedTotalPassenger.parse(json.loads(x.value))) \
                            .toDF(schema)
        
        drop_null_row_df = parse_df.na.drop()
        
        total_passenger_df = drop_null_row_df \
                            .select(col("passenger_count")) \
                            .agg({'passenger_count': 'sum'}) \
                            .toDF("passenger_count")
        
        total_passenger_df = total_passenger_df \
                            .select("*") \
                            .withColumn("total_rides", lit(total_rides)) \
                            .withColumn("created_at", lit(datetime.now())) \

        total_passenger_df \
            .write \
            .format("snowflake") \
            .options(**SNOWFLAKE_OPTIONS) \
            .option("sfSchema", "YELLOW_TAXI_SPEED") \
            .option("dbtable", "TOTAL_PASSENGER") \
            .mode("append") \
            .save()
        
        total_passenger = total_passenger_df.collect()[0][0]

        logger.info(f"Save to table yellow_taxi_speed.total_passenger ({total_passenger}, {total_rides})")
    except Exception as e:
      logger.error(e)

  @staticmethod
  def parse(raw_data):
    passenger_count = raw_data["passenger_count"] if "passenger_count" in raw_data else None

    data = { 
              'passenger_count': passenger_count
           }

    return data

  def run(self):
    try:
      df = self.comsume_from_kafka()

      stream = df \
            .writeStream \
            .foreachBatch(self.save_to_snowflake) \
            .outputMode("append") \
            .start()

      stream.awaitTermination()
    except Exception as e:
      logger.error(e)

if __name__ == '__main__':
  SpeedTotalPassenger().run()