from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # BigQuery Operator 추가
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# DAG definition
dag = DAG(
    'insert_current_time_to_bigquery',  # DAG 이름 변경
    default_args=default_args,
    description='Insert current time to BigQuery every 30 seconds',  # 설명 변경
    schedule_interval=timedelta(seconds=30),
    start_date=datetime(2024, 3, 26),
    catchup=False
)

def get_current_time():
    return datetime.now()

def load_to_bigquery():  # 함수 이름 및 설명 변경
    current_time = get_current_time()
    try:
        # Store current time in BigQuery
        # BigQuery 테이블 이름과 데이터셋 이름을 수정하여 사용합니다.
        project_id = 'fluid-crane-417212'  # 여기에 BigQuery 프로젝트 ID를 입력하세요.
        dataset_id = 'airflow_test'  # 여기에 BigQuery 데이터셋 ID를 입력하세요.
        table_id = 'airflow_test_time'  # 여기에 BigQuery 테이블 ID를 입력하세요.

        insert_job = BigQueryInsertJobOperator(
            task_id='insert_current_time_to_bigquery',  # 태스크 이름 변경
            gcp_conn_id='google_cloud_platform',  # GCP 연결 ID 변경
            project_id=project_id,
            location='asia-northeast2',  # BigQuery 테이블이 있는 리전을 입력하세요.
            dataset_id=dataset_id,
            table_id=table_id,
            configuration={
                'query': {
                    'query': f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` (current_time) VALUES ('{current_time.strftime('%Y-%m-%d %H:%M:%S')}')"
                }
            },
            dag=dag
        )

        return insert_job.execute(context=None)  # BigQuery Insert 작업 실행
    except Exception as e:
        print(f"Failed to insert into BigQuery: {str(e)}")

# Task to get current time
get_time_task = PythonOperator(
    task_id='get_current_time',
    python_callable=get_current_time,
    dag=dag
)

# Task to load data into BigQuery
load_to_bigquery_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag
)

# Define the order of task execution
get_time_task >> load_to_bigquery_task
