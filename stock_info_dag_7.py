from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import yfinance as yf
from pytz import timezone

# 한국 시간 오후 7시 KST
KST = timezone('Asia/Seoul')
KST_now = datetime.now(KST)

# 국내 유명 주식 5종목 코드
CODES = ['005930.KS', '035420.KS', '000660.KS', '051900.KS', '032830.KS']

# 오늘 날짜 설정
start_date = datetime(2024, 1, 1).strftime('%Y-%m-%d')
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
            return None
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

def load_to_bigquery(rows):
    try:
        # BigQuery 테이블 이름과 데이터셋 이름을 수정하여 사용합니다.
        project_id = 'fluid-crane-417212'  # 여기에 BigQuery 프로젝트 ID를 입력하세요.
        dataset_id = 'airflow_test'  # 여기에 BigQuery 데이터셋 ID를 입력하세요.
        table_id = 'stock_info'  # 여기에 BigQuery 테이블 ID를 입력하세요.
        stock_data = prepare_stock_data()
        for row in stock_data:
            query = f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` (code, name, date, open, high, low, close, volume) " \
                    f"VALUES ('{row['code']}', '{row['name']}', '{row['date']}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']});"

            insert_job = BigQueryExecuteQueryOperator(
                task_id=f'insert_stock_info_{row["code"]}_to_bigquery',
                sql=query,
                use_legacy_sql=False,  # BigQuery의 표준 SQL 사용
                location='asia-northeast2',
                gcp_conn_id='google_cloud_default',  # 수정된 GCP 연결 ID
                dag=dag
            )
            insert_job.execute(context=None)  # BigQuery 쿼리 실행
    except Exception as e:
        print(f"Failed to insert into BigQuery: {str(e)}")

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
    '3insert_stock_info_to_bigquery',  # DAG 이름 변경
    default_args=default_args,
    description='Insert stock info to BigQuery every day',  # 설명 변경
    schedule_interval=timedelta(days=1),  # 매일 실행
    start_date=datetime(2024, 1, 1),
    catchup=True
)

# Task to get stock info
get_stock_info_task = PythonOperator(
    task_id='get_stock_info',
    python_callable=prepare_stock_data,
    dag=dag
)

# Task to load data into BigQuery
load_to_bigquery_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    op_kwargs={'rows': prepare_stock_data()},
    dag=dag
)

# Define the order of task execution
get_stock_info_task >> load_to_bigquery_task
