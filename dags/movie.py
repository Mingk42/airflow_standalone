from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import DB from csv',
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['movie', 'etl', 'shop'],
) as dag:



    task_get_data = BashOperator(
        task_id='get.data',
        bash_command="""
            echo "get.data.start"
            echo "get.data.end"
        """
    )

    task_save_data = BashOperator(
        task_id = "save.data",
        bash_command = """
            echo "save.data.start"
            echo "save.data.end"
        """
    )

    task_middle1 = BashOperator(
            task_id="middle",
            bash_command="""
                echo "middle1.start"
                echo "middle1.end"
            """
    )

    task_middle2 = BashOperator(
            task_id="middle2",
            bash_command="""
                echo "middle2.start"
                echo "middle2.end"
            """
    )
    task_middle3 = BashOperator(
            task_id="middle3",
            bash_command = """
                echo "middle3.start"
                echo "middle3.end"
            """,
            trigger_rule="all_success"
    )

    task_middle4 = BashOperator(
            task_id="middle4",
            bash_command="""
                echo "middle4.start"
                echo "middle4.end"
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
              echo "err report"  
            """,
            trigger_rule="one_failed"
    )

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')

    task_start >> task_get_data

    #task_get_data >> task_err >> task_end    # fail flow
    task_get_data >> task_save_data >> [task_middle1, task_middle2, task_middle3, task_middle4] >> task_end   # success flow
