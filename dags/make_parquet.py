from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make parquet from DB',
    schedule = "10 5 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['parquet', 'etl', 'shop'],
) as dag:

    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
            DONE_PATH={{var.value.IMPORT_DONE_PATH}}/{{ds_nodash}}
            bash {{var.value.CHECK_SH}} $DONE_PATH
        """
    )

    task_to_parquet = BashOperator(
        task_id = "to.parquet",
        bash_command = """
            echo "to.parquet"
        """
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make done"
                
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
              echo "err report"  
            """,
            trigger_rule="one_failed"
    )

    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')

    task_start >> task_check

    task_check >> task_err >> task_end    # fail flow
    task_check >> task_to_parquet >> task_done >> task_end   # success flow
