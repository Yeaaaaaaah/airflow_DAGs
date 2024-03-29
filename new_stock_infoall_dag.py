from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
import yfinance as yf

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
    'all_insert_stock_info_to_bigquery',
    default_args=default_args,
    description='Insert stock info to BigQuery every day',
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2024, 1, 1),  # DAG 시작 날짜 변경
    end_date=datetime(2024, 3, 28),  # 종료 날짜 추가
    catchup=False
)

# 국내 유명 주식 5종목 코드
CODES = ['005930.KS', '035420.KS', '000660.KS', '051900.KS', '032830.KS']

# 주식 코드와 이름을 매핑하는 딕셔너리
stock_names = {
    '005930': '삼성전자',
    '035420': 'NAVER',
    '000660': 'SK하이닉스',
    '051900': 'LG화학',
    '032830': '삼성생명',
}

def get_stock_info(code, start_date, end_date):
    """
    주어진 종목 코드의 주가 정보를 가져옵니다.
    """
    try:
        # 종목 정보 가져오기
        data = yf.download(code, start=start_date, end=end_date)
        if data.empty:
            return []
        return data
    except Exception as e:
        print(f"종목 {code} 정보 가져오기 실패: {e}")
        return None

def insert_stock_info_to_bigquery(**kwargs):
    """
    주식 정보를 BigQuery에 삽입합니다.
    """
    row = kwargs['row']
    start_date = kwargs['start_date']
    end_date = kwargs['end_date']
    
    # BigQuery Client 라이브러리 import
    from google.cloud import bigquery

    # BigQuery Client 객체 생성
    client = bigquery.Client()

    # SQL 쿼리 작성
    insert_query = get_insert_query(row, start_date, end_date)
  
    # SQL 쿼리 실행
    try:
        # BigQuery에 데이터를 삽입하는 작업을 생성합니다.
        insert_job = BigQueryExecuteQueryOperator(
            task_id=f'insert_stock_info_to_bigquery_{row["code"]}',
            sql=insert_query,
            use_legacy_sql=False,
            location='asia-northeast2',
            gcp_conn_id='google_cloud_default',
            dag=dag
        )
        insert_job.execute(context=None)
    except Exception as e:
        print(f"Failed to insert into BigQuery: {str(e)}")

def prepare_stock_data():
    """
    모든 종목의 주가 정보를 준비합니다.
    """
    rows = []
    for code in CODES:
        info = get_stock_info(code, datetime(2024, 1, 1), datetime(2024, 3, 28))  # 변경된 날짜로 데이터 수집
        if info is not None:
            rows.append({'code': code, 'data': info})

    return rows

def get_insert_query(row, start_date, end_date):
    """
    주어진 종목 코드에 대한 SQL 삽입 쿼리를 반환합니다.
    """
    project_id = 'fluid-crane-417212'
    dataset_id = 'airflow_test'
    table_id = 'stock_info'

    # SQL 쿼리 작성
    query = f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` (code, name, date, open, high, low, close, volume) VALUES "
    
    for date, data in row['data'].iterrows():
        query += f"('{row['code'].split('.')[0]}', '{stock_names[row['code'].split('.')[0]]}', '{date.strftime('%Y-%m-%d')}', {data['Open']}, {data['High']}, {data['Low']}, {data['Close']}, {data['Volume']}), "

    # 마지막 쉼표와 공백 제거
    query = query[:-2]

    return query

# 주식데이터 집합 준비
rows = prepare_stock_data()

# TASK 생성
insert_job_list = []
for row in rows:
    t = PythonOperator(
        task_id=f"insert_stock_info_to_bigquery_{row['code']}",
        python_callable=insert_stock_info_to_bigquery,
        op_kwargs={'row': row, 'start_date': datetime(2024, 1, 1), 'end_date': datetime(2024, 3, 28)},  # 변경된 날짜 전달
        dag=dag
    )
    insert_job_list.append(t)

# TASK 간 의존성 정의
for i in range(1, len(insert_job_list)):
    insert_job_list[i] >> insert_job_list[i - 1]
