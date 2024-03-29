from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import yfinance as yf
from pytz import timezone

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
    'pyop_insert_stock_info_to_bigquery',
    default_args=default_args,
    description='Insert stock info to BigQuery every day',
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2024, 3, 20),
    catchup=False
)

# 한국 시간 오후 7시 KST
KST = timezone('Asia/Seoul')
KST_now = datetime.now(KST)

# 국내 유명 주식 5종목 코드
CODES = ['005930.KS', '035420.KS', '000660.KS', '051900.KS', '032830.KS']

# 오늘 날짜 설정
start_date = datetime(2024, 3, 20).strftime('%Y-%m-%d')
end_date = KST_now.strftime('%Y-%m-%d')

# 주식 코드와 이름을 매핑하는 딕셔너리
stock_names = {
    '005930': '삼성전자',
    '035420': 'NAVER',
    '000660': 'SK하이닉스',
    '051900': 'LG화학',
    '032830': '삼성생명',
    # 추가적인 주식 코드와 이름은 필요에 따라 추가할 수 있습니다.
}

def get_stock_info(code):
    """
    주어진 종목 코드의 주가 정보를 가져옵니다.
    """
    try:
        # 종목 정보 가져오기
        data = yf.download(code, start=start_date, end=end_date)
        if data.empty:
            return []
        return data.iloc[-1]
    except Exception as e:
        print(f"종목 {code} 정보 가져오기 실패: {e}")
        return None

def prepare_stock_data():
    """
    모든 종목의 주가 정보를 준비합니다.
    """
    rows = []
    for code in CODES:
        info = get_stock_info(code)
        if info is not None:
            row = {
                "code": code.split('.')[0],  # .KS 부분을 제외한 종목 코드
                "name": stock_names[code.split('.')[0]],  # 주식 이름
                "date": info.name.strftime('%Y-%m-%d'),  # 인덱스가 날짜인 경우 사용
                "open": info["Open"],
                "high": info["High"],
                "low": info["Low"],
                "close": info["Close"],
                "volume": info["Volume"]
            }
            rows.append(row)

    return rows


def get_insert_query(row):
  """
  주어진 종목 코드에 대한 SQL 삽입 쿼리를 반환합니다.
  """

  # BigQuery 테이블 이름과 데이터셋 이름을 수정하여 사용합니다.
  project_id = 'fluid-crane-417212'
  dataset_id = 'airflow_test'
  table_id = 'stock_info_pykrx'

  # SQL 쿼리 작성
  query  = f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` (code, name, date, open, high, low, close, volume) " \
                    f"VALUES ('{row['code']}', '{row['name']}', '{row['date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']});"

  return query

def insert_stock_info_to_bigquery(row):
  """
  주식 정보를 BigQuery에 삽입합니다.
  """

  # BigQuery Client 라이브러리 import
  from google.cloud import bigquery

  # BigQuery Client 객체 생성
  client = bigquery.Client()

  # SQL 쿼리 작성
  insert_query = get_insert_query(row)
  
  # SQL 쿼리 실행
  try:
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

# 주식데이터 집합 준비
rows = prepare_stock_data()

# TASK 생성
insert_job_list = []
for row in rows:
    t = PythonOperator(
        task_id=f"insert_stock_info_to_bigquery_{row['code']}",
        python_callable=insert_stock_info_to_bigquery,
        op_kwargs={'row': row},
        dag=dag
    )
    insert_job_list.append(t)

# TASK 간 의존성 정의
for i in range(1, len(insert_job_list)):
    insert_job_list[i] >> insert_job_list[i - 1]
