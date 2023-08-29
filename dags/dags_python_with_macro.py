import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id = "dags_python_with_macro",
    schedule = '10 0 * * *', # 매일 00시 10분
    start_date = pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    # 1. Macro를 사용한 방법
    @task(task_id='task_using_macros', 
          templates_dict = {
              'start_date': '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months = -1, day = 1)) | ds }}',
              'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day = 1) + macros.dateutil.relativedelta.relativedelta(days = -1)) | ds }}'
     }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)
            
    # 2. Python 자체 라이브러리를 사용한 방법
    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        # Tip) 가급적이면 스케줄러의 부하를 줄이기 위해 operator 안에서만 써야 하는 라이브러리는 operator 안에서 선언해 줄 것!
        from dateutil.relativedelta import relativedelta
        
        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone('Asia/Seoul') + relativedelta(months = -1, day = 1)
        prev_month_day_last = data_interval_end.in_timezone('Asia/Seoul').replace(day = 1) +  relativedelta(days = -1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))
        
    get_datetime_macro() >> get_datetime_calc()