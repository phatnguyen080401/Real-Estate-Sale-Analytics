import sys
sys.path.append("..")

import os
from datetime import datetime, timedelta
from src.custom_functions import download_file, split_file, move_file

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
Variable.setdefault("YEAR", 2022)
Variable.setdefault("MONTH", 1)
Variable.setdefault("FILE_NUMBER", 0)
Variable.setdefault("PARTITION", 800)

# Set variables
def set_variables(year, month, file_number, partition, **kwargs):
    if (file_number + 1) == partition:
        if month == 12:
            Variable.set("YEAR", year + 1)
            Variable.set("MONTH", 1)
        else:
            Variable.set("MONTH", month + 1)
        Variable.set("FILE_NUMBER", 0)
    else:
        Variable.set("FILE_NUMBER", file_number + 1)

# Get variables
def get_variables():
    year = int(Variable.get("YEAR"))
    month = int(Variable.get("MONTH"))
    file_number = int(Variable.get("FILE_NUMBER"))
    partition = int(Variable.get("PARTITION"))

    return {'year': year, 'month': month, 'file_number': file_number, 'partition': partition}

# Check if folder tmp is empty or not
def folder_is_empty():
    if not os.listdir("/opt/airflow/src/tmp"):
        return "download_file"
    return "move_file_to_folder_data"

with DAG('fetch_data_dag', default_args=default_args, catchup=False, schedule=timedelta(seconds=30)):
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
        python_callable=download_file,
        op_kwargs=variables_dict
    )
    
    # Split file
    split_file_to_partitions = PythonOperator(
        task_id="split_file",
        python_callable=split_file,
        op_kwargs=variables_dict
    )

    move_file_to_folder_data >> point_to_next_file
    download_new_file >> split_file_to_partitions
    folder_is_empty_or_not >> (move_file_to_folder_data, download_new_file)