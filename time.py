from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    'insert_current_time_to_airflow_db',
    default_args=default_args,
    description='Insert current time to Airflow metadata DB every 5 seconds',
    schedule_interval=timedelta(seconds=5),
    start_date=days_ago(1),
    catchup=False
)

def get_current_time():
    return datetime.now()

def load_to_airflow_db():
    current_time = get_current_time()
    try:
        # Store current time in Airflow's metadata database
        Variable.set("current_time", current_time.strftime('%Y-%m-%d %H:%M:%S'))
        print("Successfully inserted current time into Airflow metadata DB.")
    except Exception as e:
        print(f"Failed to insert into Airflow metadata DB: {str(e)}")

# Task to get current time
get_time_task = PythonOperator(
    task_id='get_current_time',
    python_callable=get_current_time,
    dag=dag
)

# Task to load data into Airflow metadata DB
load_to_airflow_db_task = PythonOperator(
    task_id='load_to_airflow_db',
    python_callable=load_to_airflow_db,
    dag=dag
)

# Define the order of task execution
get_time_task >> load_to_airflow_db_task
