import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_operator", # 일반적으로 DAG 파일 명과 dag_id는 일치 시켜주는 것이 좋음 (수많은 DAG 중, 필요한 DAG를 바로 찾을 수 있도록 해주기 위함)
    schedule = "0 0 * * *", # "분 시 일 월 요일"
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"), # 반드시 한국 시간으로 설정할 것
    catchup = False # 설정한 start_date부터 현재 일자 사이의 기간을 follow-up 할 것인지 여부 (단, 해당 구간을 차례차례 돌지 않고 한꺼번에 돌기 때문에 문제가 발생할 수도 있음)
    # dagrun_timeout = datetime.timedelta(minutes = 60), # ex) 60분 이상 돌면 실패나도록 설정
    # tags = ['example', 'example2'], # 태그 별로 추려서 보고자 하는 경우 사용하면 좋은 option
    # params = {'example_key': 'example_value'} # Task들에 공통적으로 넘겨줄 파라미터가 존재할 경우 사용하는 option
) as dag:
    # Task 객체 설정
    bash_t1 = BashOperator(
        task_id = "bash_t1", # 일반적으로 Task 객체 명과 task_id는 일치 시켜주는 것이 좋음 (나중에 Task를 찾기 쉽도록 하기 위함)
        bash_command = "echo whoami",
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        bash_command = "echo $HOSTNAME",
    )

    bash_t1 >> bash_t2 # Task 실행 순서 지정