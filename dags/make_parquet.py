from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make parquet from csv',
    schedule = "10 2 * * *",
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
        
            # READ_PATH=      ~~~        파이썬코드에 넣었음.
            # SAVE_PATH=       ~~~       파이썬코드에 넣었음. 파티셔닝 도입하면 불필요한 행

            python {{var.value.PY_PATH}}/csv2parquet.py {{ds_nodash}}
        """
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make.done"
                DONE_PATH={{var.value.MKPARQUET_DONE_PATH}}/{{ds_nodash}}
                mkdir -p $DONE_PATH
                
                touch $DONE_PATH/_DONE
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
              echo "err report"  
            """,
            trigger_rule="one_failed"
    )

    # task_start = EmptyOperator(task_id='start')
    task_start = gen_emp("g_start") 
    task_end = gen_emp("g_end", "all_done")
    # task_end = EmptyOperator(task_id='end', trigger_rule='all_done')

    task_start >> task_check

    task_check >> task_err >> task_end    # fail flow
    task_check >> task_to_parquet >> task_done >> task_end   # success flow
