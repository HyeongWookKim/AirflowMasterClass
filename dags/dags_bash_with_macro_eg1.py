import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_macro_eg1",
    schedule = '10 0 L * *', # 매월 말일 00시 10분
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id = 'bash_task_1',
        env = {
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}', # 한국 시간으로 맞춰주기 위해 in_timezone("Asia/Seoul") 사용
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 1)) | ds }}'
        },
        bash_command = 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )