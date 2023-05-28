from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from custom_functions import run_gx_checkpoint

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023,3,20),
  'retries': 0
}

with DAG('batch_layer_dag', default_args=default_args, catchup=False, max_active_runs=1, schedule="*/20 * * * *") as dag:
  total_customer_by_property_type_batch = BashOperator(
    task_id="total_customer_by_property_type_batch",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_customer_by_property_type_batch.py"
  )

  total_customer_by_property_type_validation = PythonOperator(
    task_id="total_customer_by_property_type_validation",
    python_callable=run_gx_checkpoint,
    op_kwargs={"checkpoint_name": "total_customer_by_property_type_checkpoint"}
  )

  total_customer_by_town_batch = BashOperator(
    task_id="total_customer_by_town_batch",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_customer_by_town_batch.py"
  )

  total_customer_by_town_validation = PythonOperator(
    task_id="total_customer_by_town_validation",
    python_callable=run_gx_checkpoint,
    op_kwargs={"checkpoint_name": "total_customer_by_town_checkpoint"}
  )

  total_sale_amount_ratio_batch = BashOperator(
    task_id="total_sale_amount_ratio_batch",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_layer/total_sale_amount_ratio_batch.py"
  )

  total_sale_amount_ratio_validation = PythonOperator(
    task_id="total_sale_amount_ratio_validation",
    python_callable=run_gx_checkpoint,
    op_kwargs={"checkpoint_name": "total_sale_amount_ratio_checkpoint"}
  )

  end = EmptyOperator(task_id="done", trigger_rule='all_success')

  (
    total_customer_by_property_type_batch >> total_customer_by_property_type_validation,
    total_customer_by_town_batch >> total_customer_by_town_validation,
    total_sale_amount_ratio_batch >> total_sale_amount_ratio_validation
  ) >> end