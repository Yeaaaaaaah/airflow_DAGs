from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple DAG to test Airflow functionality',
    schedule_interval=timedelta(minutes=1),  # 매 분마다 실행
    start_date=datetime(2024, 3, 26),
    catchup=False
)

# 현재 시간을 출력하는 Python 함수
def print_current_time():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f'현재 시간: {current_time}')

# PythonOperator를 사용하여 작업 정의
print_time_task = PythonOperator(
    task_id='print_current_time',
    python_callable=print_current_time,
    dag=dag
)

# 작업 실행 순서 정의
print_time_task
