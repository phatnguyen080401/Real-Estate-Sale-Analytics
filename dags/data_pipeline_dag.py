from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,3,15),
    'retries': 0,
    'catchup': False,
}

with DAG('data_pipeline_dag', default_args=default_args, schedule="@daily") as dag:
    save_to_data_lake = BashOperator(
        task_id="save_to_data_lake",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/kafka_jobs/test.py"
    )

    end = DummyOperator(task_id="none", trigger_rule='all_done')

    save_to_data_lake >> end