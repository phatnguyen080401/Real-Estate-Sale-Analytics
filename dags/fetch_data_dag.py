import glob
from datetime import datetime, timedelta
from custom_functions import (
  download_file, 
  split_file, 
  move_file, 
  convert_to_parquet, 
  adjust_dataframe
)

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator

# default arguments
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023,3,20),
  'retries': 0
}

# Default variable in Airflow UI (Admin -> Variables)
Variable.setdefault("FILE_NUMBER", 0)
Variable.setdefault("PARTITION", 4000)

# Set variables
def set_variables(file_number, partition, **kwargs):
  if (file_number + 1) == partition:
    Variable.set("FILE_NUMBER", 0)
  else:
    Variable.set("FILE_NUMBER", file_number + 1)

# Get variables
def get_variables():
  file_number = int(Variable.get("FILE_NUMBER"))
  partition = int(Variable.get("PARTITION"))

  return {
          'file_number': file_number, 
          'partition': partition
         }

# Check if folder tmp is empty or not
def folder_is_empty():
  if not glob.glob("/opt/airflow/src/tmp/*.parquet"):
    return "download_file"
  return "move_file_to_folder_data"

with DAG('fetch_data_dag', default_args=default_args, catchup=False, max_active_runs=1, schedule="*/1 * * * *"):
  variables_dict = get_variables()

  # Check if the folder is empty or not
  folder_is_empty_or_not = BranchPythonOperator(
    task_id="folder_is_empty",
    python_callable=folder_is_empty
  )

  # Move file from folder tmp to data
  move_file_to_folder_data = PythonOperator(
    task_id="move_file_to_folder_data",
    python_callable=move_file,
    op_kwargs=variables_dict
  )

  # Point to the next file to move
  point_to_next_file = PythonOperator(
    task_id="point_to_next_file",
    python_callable=set_variables,
    op_kwargs=variables_dict
  )

  # Download next file based on month and year
  download_new_file = PythonOperator(
    task_id="download_file",
    python_callable=download_file
  )

  # Convert csv to parquet
  convert_to_parquet_file = PythonOperator(
    task_id="convert_to_parquet",
    python_callable=convert_to_parquet
  )

  # Change data type object to correct data type
  change_data_type = PythonOperator(
    task_id="adjust_dataframe",
    python_callable=adjust_dataframe
  )
    
  # Split file
  split_file_to_partitions = PythonOperator(
    task_id="split_file",
    python_callable=split_file,
    op_kwargs=variables_dict
  )

  move_file_to_folder_data >> point_to_next_file
  download_new_file >> convert_to_parquet_file >> change_data_type >> split_file_to_partitions
  folder_is_empty_or_not >> (move_file_to_folder_data, download_new_file)