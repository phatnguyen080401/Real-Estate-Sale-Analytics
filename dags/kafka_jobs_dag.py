from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
  "owner": 'airflow',
  "depends_on_past": False,
  "start_date": datetime(2023,3,20),
  "retries": 5,
  "retry_delay": timedelta(minutes=1)
}

with DAG('kafka_jobs_dag', default_args=default_args, catchup=False, schedule='@once') as dag:
  producer = BashOperator(
    task_id="producer",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/kafka_jobs/producer.py"
  )

  consumer = BashOperator(
    task_id="consumer",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/kafka_jobs/consumer.py"
  )

  end = EmptyOperator(task_id="done")
    
  (producer, consumer) >> end
