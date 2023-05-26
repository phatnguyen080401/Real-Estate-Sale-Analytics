import time
import shutil
import dask.dataframe as dd
import pandas as pd
import numpy as np
import subprocess

from pyarrow import *
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

from urllib.error import HTTPError
from urllib import request

from airflow.exceptions import AirflowException

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
                    field("serial_number", int64(), True),
                    field("list_year", int64(), True),
                    field("date_recorded", date64(), True),
                    field("town", string(), True),
                    field("address", string(), True),
                    field("assessed_value", float64(), True),
                    field("sale_amount", float64(), True),
                    field("sales_ratio", float64(), True),
                    field("property_type", string(), True),
                    field("residential_type", string(), True),
                    field("non_use_code", string(), True),
                    field("assessor_remarks", string(), True),
                    field("opm_remarks", string(), True),
                    field("location", string(), True)
            ])

  name_function = lambda x: f"Real_Estate_Sales-{x}.parquet"
  ddf = dd.read_parquet(f"{AIRFLOW_DIR}/src/data_source/{FILE_NAME}.parquet")
  sorted_ddf = ddf.sort_values("date_recorded")
  sorted_ddf.repartition(partition).to_parquet(f"{AIRFLOW_DIR}/src/tmp/", name_function=name_function, schema=schema)

def move_file(file_number, **kwargs):
  source = "tmp"
  destination = "data"
  file_name = f"Real_Estate_Sales-{file_number}"
  shutil.move(f"{AIRFLOW_DIR}/src/{source}/{file_name}.parquet", 
              f"{AIRFLOW_DIR}/src/{destination}/{file_name}.parquet")

def run_gx_checkpoint(checkpoint_name, **kwargs):
  validation_process = subprocess.Popen(
    ["great_expectations", "checkpoint", "run", checkpoint_name],
    stdout=subprocess.PIPE,
    universal_newlines=True,
  )
  validation_result = validation_process.communicate()[0]
  return_code = validation_process.returncode

  if return_code:
    docs_process = subprocess.Popen(
      ["great_expectations", "docs", "list"],
      stdout=subprocess.PIPE,
      universal_newlines=True,
    )
    docs_result = docs_process.communicate()[0]

    print(docs_result)

    raise AirflowException(
      "Checkpoint validation failed. Inspect the Data Docs for more information."
    )
  else:
    print(validation_result)