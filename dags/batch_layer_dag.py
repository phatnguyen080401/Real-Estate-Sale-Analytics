from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,3,20),
    'retries': 0
}

with DAG('batch_layer_dag', default_args=default_args, catchup=False, schedule="*/10 * * * *") as dag:
    total_amount_batch = BashOperator(
        task_id="total_amount_batch",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_amount_batch.py"
    )

    total_passenger_batch = BashOperator(
        task_id="total_passenger_batch",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_passenger_batch.py"
    )

    total_trip_distance_batch = BashOperator(
        task_id="total_trip_distance_batch",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_trip_distance_batch.py"
    )

    end = EmptyOperator(task_id="done", trigger_rule='all_success')

    (total_amount_batch,total_passenger_batch,total_trip_distance_batch) >> end