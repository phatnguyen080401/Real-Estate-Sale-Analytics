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

with DAG('speed_layer_dag', default_args=default_args, catchup=False, schedule='@once') as dag:
    pickup_dropoff_speed = BashOperator(
        task_id="pickup_dropoff_speed",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/pickup_dropoff_speed.py"
    )

    user_per_payment_speed = BashOperator(
        task_id="user_per_payment_speed",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/user_per_payment_speed.py"
    )

    total_amount_speed = BashOperator(
        task_id="total_amount_speed",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_amount_speed.py"
    )

    total_passenger_speed = BashOperator(
        task_id="total_passenger_speed",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_passenger_speed.py"
    )

    total_trip_distance_speed = BashOperator(
        task_id="total_trip_distance_speed",
        bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_trip_distance_speed.py"
    )

    end = EmptyOperator(task_id="done")
    
    (pickup_dropoff_speed, user_per_payment_speed, total_amount_speed, total_passenger_speed, total_trip_distance_speed) >> end
    