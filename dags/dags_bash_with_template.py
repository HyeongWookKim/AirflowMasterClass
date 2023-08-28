import datetime, pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_template",
    schedule = '10 0 * * *', # 매일 00시 10분
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = 'echo "data_interval_end: {{ data_interval_end }}"'
    )
    
    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            'START_DATE': '{{ data_interval_start | ds }}', # YYYY-MM-DD 형태로 반환되도록 ds 옵션 추가
            'END_DATE': '{{ data_interval_end | ds }}' # YYYY-MM-DD 형태로 반환되도록 ds 옵션 추가
        },
        bash_command = 'echo $START_DATE && echo $END_DATE' # &&는 앞에 있는 command가 성공하면 뒤에 있는 command를 실행하겠다는 것을 의미함 (Shell Script 문법)
    )
    
    bash_t1 >> bash_t2