from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator


with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    description='movie summary',
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 20),
    catchup=True,
    tags=['movie', 'summary', 'etl', 'shop'],
) as dag:

    
    task_apply_type=EmptyOperator(task_id="apply.type")
    task_merge_df=EmptyOperator(task_id="merge.df")
    task_de_dup=EmptyOperator(task_id="de.dup")
    task_summary_df=EmptyOperator(task_id="summary.df")
    """
    task_apply_type=PythonVirtualenvOperator(task_id="apply.type")
    task_merge_df=PythonVirtualenvOperator(task_id="merge.df")
    task_de_dup=PythonVirtualenvOperator(task_id="de.dup")
    task_summary_df=PythonVirtualenvOperator(task_id="summary.df")
    """
    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')


    task_start >> task_apply_type >> task_merge_df >> task_de_dup >> task_summary_df >> task_end
