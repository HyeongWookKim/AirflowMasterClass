import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id = "dags_python_with_op_args",
    schedule = '30 6 * * *', # 매일 06시 30분
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = 'regist_t1',
        python_callable = regist,
        op_args = ['brian', 'man', 'kr', 'seoul'] # 넘겨줄 파라미터 값을 리스트로 지정
    )
    
    regist_t1