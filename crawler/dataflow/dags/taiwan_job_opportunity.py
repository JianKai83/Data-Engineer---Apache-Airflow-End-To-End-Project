import airflow

from dataflow.constant import (
    DEFAULT_ARGS,
    MAX_ACTIVE_RUNS,
)
from dataflow.etl.taiwan_job_opportunity import (
    generate_104_tasks,generate_1111_tasks
)
from dataflow.analysis.analysis import Update_Chart
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME="job_opportunity"

with airflow.DAG(
    dag_id=DAG_NAME,
    default_args=DEFAULT_ARGS,
    # 設定每天 17:00 執行爬蟲
    schedule_interval="0 17 * * *",
    max_active_runs=MAX_ACTIVE_RUNS,
    # 設定參數，Airflow 除了按按鈕，單純的執行外
    # 也可以在按按鈕時，帶入特定參數
    # 在此設定 date 參數，讓讀者可自行輸入，想要爬蟲的日期
    params={
        "searchElement":""
    },
    catchup=False,
) as dag:
    start_task = DummyOperator(
        task_id="start_task"
    )

    end_task = DummyOperator(
        task_id="end_task"
    )

    subdag_1 = SubDagOperator(
        task_id = 'subdag-1',
        subdag= generate_104_tasks(DAG_NAME,'subdag-1',DEFAULT_ARGS),
    )

    subdag_2 = SubDagOperator(
        task_id = 'subdag-2',
        subdag= generate_1111_tasks(DAG_NAME,'subdag-2',DEFAULT_ARGS),
    )

    check_task = DummyOperator(
        task_id='check'
    )
    
    analysis_task = PythonOperator(
        task_id='analysis',
        python_callable=Update_Chart,
        queue="job_104" # 試試看在 default_args 中設定一個 queue 來處理
    )
    (
        start_task
        >> subdag_1
        >> check_task
        >> subdag_2
        >> analysis_task
        >> end_task
    )