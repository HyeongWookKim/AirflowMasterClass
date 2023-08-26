import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id = "dags_python_with_op_kwargs",
    schedule = '30 6 * * *', # 매일 06시 30분
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable = regist2,
        op_args = ['brian', 'man', 'kr', 'seoul'],
        op_kwargs = {'email': 'brian@hanmail.net', 'phone': '010-1234-5678'}
    )
    
    regist2_t1