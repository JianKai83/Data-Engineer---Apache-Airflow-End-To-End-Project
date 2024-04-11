from airflow.models import DAG
from airflow.operators.python_operator import (
    PythonOperator,
)

from dataflow.backend import db
from dataflow.crawler.taiwan_job_opportunity import (
    crawler,
)


def crawler_job_tw_104(pageNum,
    **kwargs,
):
    # 由於在 DAG 層，設定 params，可輸入參數
    # 因此在此，使用以下 kwargs 方式，拿取參數
    # DAG 中 params 參數設定是 date (YYYY-MM-DD)
    # 所以拿取時，也要用一樣的字串
    params = kwargs["dag_run"].conf
    searchElement = params.get("searchElement", "資料工程師")
    # 進行爬蟲
    df = crawler(
        dict(
            pageNum=pageNum,
            data_source="job_104",
            searchElement = searchElement
        )
    )
    # 資料上傳資料庫
    db.upload_data(
        df,
        "job_104",
        db.router.mysql_jobdata_conn,
    )


def crawler_job_tw_1111(pageNum,
    **kwargs,
):
    # 註解如上
    params = kwargs["dag_run"].conf
    searchElement = params.get("searchElement", "資料工程師")
    df = crawler(
        dict(
            pageNum=pageNum,
            data_source="job_1111",
            searchElement = searchElement
        )
    )
    db.upload_data(
        df,
        "job_1111",
        db.router.mysql_jobdata_conn,
    )


def generate_104_tasks(parent_dag_name, child_dag_name, default_args):

    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name), # %s 是字串格式化的方法，和 format 功能效果一樣
        default_args=default_args
    ) as dag:

        for i in range(20):
            PythonOperator(
                task_id='%s-task-%s' % (child_dag_name, i + 1),
                python_callable=crawler_job_tw_104,
                op_args=[str(i)],
                queue="job_104",
                provide_context=True,
            )

    return dag

def generate_1111_tasks(parent_dag_name, child_dag_name, default_args):

    with DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=default_args
    ) as dag:

        for i in range(20):
            PythonOperator(
                task_id='%s-task-%s' % (child_dag_name, i + 1), 
                python_callable=crawler_job_tw_1111,
                op_args=[str(i)],
                queue="job_1111",
                provide_context=True,
            )

    return dag

