import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertOperator
from pytz import timezone
from datetime import datetime, timedelta

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
    5가지 종목의 주가 정보를 준비합니다.
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

# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': KST_now,  # DAG의 시작 날짜
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    "schedule_interval": None,  # 수동 실행
}

with DAG(
    dag_id="stock_info_to_bigquery",
    default_args=default_args,
    description="주가 정보를 BigQuery에 적재합니다.",
) as dag:

    # 주가 정보 준비
    prepare_stock_data_task = PythonOperator(
        task_id="prepare_stock_data",
        python_callable=prepare_stock_data,
    )

    # BigQuery에 데이터 적재
    bigquery_insert_task = BigQueryInsertOperator(
        task_id="bigquery_insert",
        table="airflow_test.stock_info",
        source=prepare_stock_data_task.output,
        write_disposition="WRITE_TRUNCATE",  # 테이블을 먼저 비웁니다.
    )

    # 작업 연결
    prepare_stock_data_task >> bigquery_insert_task

# DAG 실행
dag.run()
