import time
import shutil
import pyarrow as pa
import dask.dataframe as dd

from pyarrow import *
from urllib.error import HTTPError
from urllib import request

def download_file(year, month, **kwargs):
  file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}"
  url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}.parquet"
  
  try:
      request.urlretrieve(url, f"/opt/airflow/src/data_source/{file_name}.parquet")
  except HTTPError:
      print(f"File {file_name}.parquet not exist") 

def split_file(year, month, partition, **kwargs):
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
  name_function = lambda x: f"{file_name}-{x}.parquet"
  ddf = dd.read_parquet(f"/opt/airflow/src/data_source/{file_name}.parquet")
  sorted_ddf = ddf.sort_values("tpep_pickup_datetime")
  sorted_ddf.repartition(partition).to_parquet("/opt/airflow/src/tmp/", name_function=name_function, schema=schema)

def move_file(year, month, file_number, **kwargs):
  source = "tmp"
  destination = "data"
  file_name = f"yellow_tripdata_{year}-{month if month > 9 else '0' + str(month)}-{file_number}"
  shutil.move(f"/opt/airflow/src/{source}/{file_name}.parquet", f"/opt/airflow/src/{destination}/{file_name}.parquet")
