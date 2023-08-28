import datetime, pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id = "dags_python_show_templates",
    schedule = '30 9 * * *', # 매일 09시 30분
    start_date = pendulum.datetime(2023, 8, 10, tz = "Asia/Seoul"),
    catchup = True
) as dag:
    
    @task(task_id = 'python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs) # 파라미터를 안 넘겨도 기본적으로 반환되는 값이 있음
        
    show_templates()