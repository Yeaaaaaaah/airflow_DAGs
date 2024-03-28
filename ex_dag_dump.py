from datetime import datetime, timedelta
from time import sleep

from airflow import DAG
from airflow.operators.python import PythonOperator

def dump():
    sleep(3)

dag_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="test_for",
    default_args=dag_args,
    start_date=datetime(2022, 1, 20),
    schedule_interval="@once",
)

# TASK 생성
list_task = list()
for i in range(3):
    t = PythonOperator(task_id=f"task_{i}", python_callable=dump, dag=dag)
    list_task.append(t)
