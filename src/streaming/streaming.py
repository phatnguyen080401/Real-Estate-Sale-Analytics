import sys
sys.path.append(".")

import time
import pyarrow as pa
from pyarrow import *
from helper import Helper

helper = Helper.getHelper()

def download_file(year, month):
  file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}"
  url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}.parquet"
  helper.download_from_url(file_name, url)

def split_file(year, month, partition=100):
  schema = pa.schema([
                    field("VendorID", int64(), True),
                    field("tpep_pickup_datetime", timestamp('s'), True),
                    field("tpep_dropoff_datetime", timestamp('s'), True),
                    field("passenger_count", float64(), True),
                    field("trip_distance", float64(), True),
                    field("RatecodeID", float64(), True),
                    field("store_and_fwd_flag", string(), True),
                    field("PULocationID", int64(), True),
                    field("DOLocationID", int64(), True),
                    field("payment_type", int64(), True),
                    field("fare_amount", float64(), True),
                    field("extra", float64(), True),
                    field("mta_tax", float64(), True),
                    field("tip_amount", float64(), True),
                    field("tolls_amount", float64(), True),
                    field("improvement_surcharge", float64(), True),
                    field("total_amount", float64(), True),
                    field("congestion_surcharge", float64(), True),
                    field("airport_fee", float64(), True)
            ])

  file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}"
  helper.split_file(file_name, schema=schema, partition=partition)

def move_file(source, destination, year, month, num_file=100):
  num = 0
  while  num < num_file:
    file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}-{num}"
    helper.move_file(source, destination, file_name)
    num += 1
    time.sleep(30)

if __name__ == "__main__":
  year = 2022
  month = 1

  # split_file(2022, 1, 800)
  # download_file(2022, 5)

  while year <= 2022 and month <= 10:
    move_file(source="tmp", 
              destination="data", 
              year=year, 
              month=month, 
              num_file=800)
    month += 1