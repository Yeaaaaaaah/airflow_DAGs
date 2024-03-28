from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from pykrx import stock
from pytz import timezone
from google.cloud import bigquery

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),  # DAG의 시작 날짜
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'update_stock_info',
    default_args=default_args,
    description='Update stock information to BigQuery',
    schedule_interval='@daily',  # 매일 실행
)

# BigQuery 클라이언트
client = bigquery.Client()

def get_stock_info(code):
    """
    주어진 종목 코드의 주가 정보를 가져옵니다.
    """
    try:
        # 현재 시간 가져오기
        KST = timezone('Asia/Seoul')
        KST_now = datetime.now(KST)
        
        # 종목 정보 가져오기
        data = stock.get_market_ohlcv_by_date(code, KST_now.strftime('%Y%m%d'), KST_now.strftime('%Y%m%d'))
        if data.empty:
            return None
        return data.iloc[-1]
    except Exception as e:
        print(f"종목 {code} 정보 가져오기 실패: {e}")
        return None

def update_stock_info():
    """
    5가지 종목의 주가 정보를 BigQuery에 업데이트합니다.
    """
    # 국내 유명 주식 5종목 코드
    CODES = ['005930', '035420', '000660', '051900', '032830']
    
    dataset_id = "airflow_test"
    table_id = "fluid-crane-417212.airflow_test.stock_info"

    rows = []
    for code in CODES:
        info = get_stock_info(code)
        if info is not None:
            row = {
                "code": code,
                "date": KST_now.strftime('%Y-%m-%d'),  # 현재 날짜로 변경
                "open": info["시가"],
                "high": info["고가"],
                "low": info["저가"],
                "close": info["종가"],
                "volume": info["거래량"]
            }
            rows.append(row)

    return rows

# PythonOperator를 사용하여 데이터 가져오는 작업 추가
fetch_stock_data_task = PythonOperator(
    task_id='fetch_stock_data_task',
    python_callable=update_stock_info,
    dag=dag,
)

# BigQueryExecuteQueryOperator를 사용하여 데이터 삽입하는 작업 추가
insert_stock_data_task = BigQueryExecuteQueryOperator(
    task_id='insert_stock_data_task',
    sql='''
    INSERT INTO `fluid-crane-417212.airflow_test.stock_info`
    (code, date, open, high, low, close, volume)
    VALUES 
    ('{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['code'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['date'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['open'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['high'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['low'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['close'] }}', 
     '{{ task_instance.xcom_pull(task_ids="fetch_stock_data_task")[0]['volume'] }}')
    ''',
    location='asia-northeast2',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# 작업 간 의존성 설정
fetch_stock_data_task >> insert_stock_data_task
