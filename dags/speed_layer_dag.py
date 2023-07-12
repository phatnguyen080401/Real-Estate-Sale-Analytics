from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023,3,20),
  'retries': 5,
  "retry_delay": timedelta(minutes=1)
}

with DAG('speed_layer_dag', default_args=default_args, catchup=False, schedule='@once') as dag:
  total_customer_by_property_type_speed = BashOperator(
    task_id="total_customer_by_property_type_speed",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_customer_by_property_type_speed.py"
  )

  total_customer_by_town_speed = BashOperator(
    task_id="total_customer_by_town_speed",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_customer_by_town_speed.py"
  )

  total_sale_amount_ratio_speed = BashOperator(
    task_id="total_sale_amount_ratio_speed",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/speed_layer/total_sale_amount_ratio_speed.py"
  )

  end = EmptyOperator(task_id="done")
    
  (total_customer_by_property_type_speed, total_customer_by_town_speed, total_sale_amount_ratio_speed) >> end
    