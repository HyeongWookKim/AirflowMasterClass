import datetime, pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id = "dags_python_template",
    schedule = '30 9 * * *', # 매일 09시 30분
    start_date = pendulum.datetime(2023, 8, 10, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    # 1. Jinja 템플릿 문법을 사용한 방법
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)
        
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = python_function1,
        op_kwargs = {'start_date': '{{ data_interval_start | ds }}', 'end_date': '{{ data_interval_end | ds }}'}
    )
    
    # 2. Jinja 템플릿 변수들을 직접 꺼내서 사용하는 방법
    @task(task_id = 'python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))
        
    python_t1 >> python_function2() # task decorator 사용 시, 이와 같이 task 순서를 지정해줘도 연결이 됨