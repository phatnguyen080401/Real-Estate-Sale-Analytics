import time
import shutil
import dask.dataframe as dd
import pandas as pd
import numpy as np

from pyarrow import *
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

from urllib.error import HTTPError
from urllib import request

FILE_NAME = "Real_Estate_Sales_2001-2020_GL"
AIRFLOW_DIR = "/opt/airflow"

def download_file():
  url = f"https://data.ct.gov/api/views/5mzw-sjtu/rows.csv?accessType=DOWNLOAD"
  
  try:
      request.urlretrieve(url, f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.csv")
  except HTTPError:
      print(f"File {FILE_NAME}.csv not exist")

def convert_to_parquet():
  df = pv.read_csv(f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.csv")
  pq.write_table(df, f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.csv".replace('csv', 'parquet'))

def adjust_dataframe():
  df = pd.read_parquet(f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.parquet")
  
  # Change data type object
  df["Date Recorded"] = df["Date Recorded"].astype('datetime64[s]')
  df["Town"] = df["Town"].astype("string")
  df["Address"] = df["Address"].astype("string")
  df["Property Type"] = df["Property Type"].astype("string")
  df["Residential Type"] = df["Residential Type"].astype("string")
  df["Non Use Code"] = df["Non Use Code"].astype("string")
  df["Assessor Remarks"] = df["Assessor Remarks"].astype("string")
  df["OPM remarks"] = df["OPM remarks"].astype("string")
  df["Location"] = df["Location"].astype("string")

  # Rename columns
  cols = {}
  for column in df.columns:
    cols[column] = column.replace(" ", "_").lower()
  rename_cols_df = df.rename(columns=cols)

  # Fill na to empty cells
  fill_na_df = rename_cols_df.replace(r'', np.NaN, regex=True)
  fill_na_df.to_parquet(f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.parquet")

def split_file(partition, **kwargs):
  schema = pa.schema([
                    field("Serial Number", int64(), True),
                    field("List Year", int64(), True),
                    field("Date Recorded", date64(), True),
                    field("Town", string(), True),
                    field("Address", string(), True),
                    field("Assessed Value", float64(), True),
                    field("Sale Amount", float64(), True),
                    field("Sales Ratio", float64(), True),
                    field("Property Type", string(), True),
                    field("Residential Type", string(), True),
                    field("Non Use Code", string(), True),
                    field("Assessor Remarks", string(), True),
                    field("OPM remarks", string(), True),
                    field("Location", string(), True)
            ])

  name_function = lambda x: f"Real_Estate_Sales-{x}.parquet"
  ddf = dd.read_parquet(f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.parquet")
  sorted_ddf = ddf.sort_values("Date Recorded")
  sorted_ddf.repartition(partition).to_parquet(f"{AIRFLOW_DIR}/src/tmp/", name_function=name_function, schema=schema)

def move_file(file_number, **kwargs):
  source = "tmp"
  destination = "data"
  file_name = f"Real_Estate_Sales-{file_number}"
  shutil.move(f"{AIRFLOW_DIR}/src/{source}/{file_name}.parquet", 
              f"{AIRFLOW_DIR}/src/{destination}/{file_name}.parquet")
