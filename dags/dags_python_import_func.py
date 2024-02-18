import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow는 "/opt/airflow/plugins" 폴더까지 sys.path로 잡힌 상태임
# 따라서 ".env" 파일을 하나 만들어서 plugins 폴더까지 PYTHONPATH로 잡히도록 설정해줘야 함
# 그렇지 않으면 from plugins.common.common_func import get_sftp 이런 식으로 불러와야 하는데, 그러면 Airflow 실행 시에 path 가 안 맞아서 에러가 발생함
from common.common_func import get_sftp

with DAG(
    dag_id = 'dags_python_import_func',
    schedule = '30 6 * * *', # 매일 06시 30분
    start_date = pendulum.datetime(2023, 3, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id = 'task_get_sftp',
        python_callable = get_sftp
    )