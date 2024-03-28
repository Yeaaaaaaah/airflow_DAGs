from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from pytz import timezone
import yfinance as yf

# 한국 시간 오후 7시 KST
KST = timezone('Asia/Seoul')
KST_now = datetime.now(KST)

# 국내 유명 주식 5종목 코드
CODES = ['005930.KS', '035420.KS', '000660.KS', '051900.KS', '032830.KS']

# 오늘 날짜 설정
start_date = (KST_now - timedelta(days=1)).strftime('%Y-%m-%d')
end_date = KST_now.strftime('%Y-%m-%d')

# 주식 코드와 이름을 매핑하는 딕셔너리
stock_names = {
    '005930': '삼성전자',
    '035420': 'NAVER',
    '000660': 'SK하이닉스',
    '051900': 'LG화학',
    '032830': '삼성생명',
}

def get_stock_info(code):
    """
    주어진 종목 코드의 주가 정보를 가져옵니다.
    """
    try:
        # 종목 정보 가져오기
        data = yf.download(code, start=start_date, end=end_date)
        if data.empty:
            return None
        return data.iloc[-1]
    except Exception as e:
        print(f"종목 {code} 정보 가져오기 실패: {e}")
        return None

def load_to_bigquery():
    try:
        project_id = 'fluid-crane-417212'  # BigQuery 프로젝트 ID
        dataset_id = 'airflow_test'  # BigQuery 데이터셋 ID
        table_id = 'stock_info'  # BigQuery 테이블 ID

        # 삽입할 데이터를 준비합니다.
        rows = prepare_stock_data()

        # 데이터를 BigQuery에 삽입하는 쿼리를 생성합니다.
        insert_query = f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` (code, name, date, open, high, low, close, volume) VALUES "
        for row in rows:
            values = f"('{row['code']}', '{row['name']}', '{row['date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']})"
            insert_query += values + ", "
        insert_query = insert_query[:-2]  # 마지막 쉼표 및 공백 제거

        # BigQuery에 데이터를 삽입하는 작업을 생성합니다.
        insert_job = BigQueryExecuteQueryOperator(
            task_id='insert_stock_info_to_bigquery',
            sql=insert_query,
            use_legacy_sql=False,
            location='asia-northeast2',
            gcp_conn_id='google_cloud_default',
            dag=dag
        )
        insert_job.execute(context=None)
    except Exception as e:
        print(f"Failed to insert into BigQuery: {str(e)}")

# Airflow DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'schedule_interval': None,  # 수동으로만 실행
}

dag = DAG(
    'insert_stock_info_to_bigquery',  # DAG 이름 변경
    default_args=default_args,
    description='Insert stock info to BigQuery every day',  # 설명 변경
    start_date=datetime(2024, 3, 26),  # 시작 날짜 변경
    catchup=False
)

# Task to load data into BigQuery
load_to_bigquery_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag
)

# Define the order of task execution
load_to_bigquery_task
