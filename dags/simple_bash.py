from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    'simple_bash',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
#    schedule_interval=timedelta(days=1),              deprecated
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_date = BashOperator(
        task_id='print.date',
        bash_command="""
            echo "data => `date`"
            echo "ds => {{ds}}"
            echo "ds_nodash => {{ds_nodash}}"
            echo "logical_date => {{logical_date}}"
            echo "logical_date => {{logical_date.strftime("%Y-%m-%d %H:%M:%S")}}"
            echo "execution_date => {{execution_date}}"
            echo "execution_date => {{execution_date.strftime('%Y-%m-%d %H:%M:%S')}}"
            echo "next_execution_date => {{next_execution_date.strftime('%Y-%m-%d %H:%M:%S')}}"
            echo "prev_execution_date => {{prev_execution_date.strftime('%Y-%m-%d %H:%M:%S')}}"
            echo "ts => {{ts}}"
        """
    )

    task_copy = BashOperator(
        task_id = "copy.log",
        bash_command = """
            mkdir -p ~/data/{{logical_date.strftime('%y%m%d')}}
            cp ~/log/history_{{logical_date.strftime('%y%m%d')}}*.log ~/data/{{logical_date.strftime('%y%m%d')}}
        """
    )

    task_sort = BashOperator(
            task_id="sort.log",
            bash_command="""
                echo "sort"
                 mkdir -p ~/data/sort/{{logical_date.strftime('%y%m%d')}}
                 cat  ~/data/cut/{{logical_date.strftime('%y%m%d')}}/cut.log | sort > ~/data/sort/{{logical_date.strftime('%y%m%d')}}/sort.log
            """
    )

    task_cut = BashOperator(
            task_id="cut.log",
            bash_command = """
                echo "cut"
                mkdir -p ~/data/cut/{{logical_date.strftime('%y%m%d')}}
                cat ~/data/{{logical_date.strftime('%y%m%d')}}/* | cut -d' ' -f1 > ~/data/cut/{{logical_date.strftime('%y%m%d')}}/cut.log
            """,
            trigger_rule="all_success"
    )

    task_count = BashOperator(
            task_id="count.log",
            bash_command="""
                echo "count"
                mkdir -p ~/data/count/{{logical_date.strftime('%y%m%d')}}
                cat  ~/data/sort/{{logical_date.strftime('%y%m%d')}}/sort.log | uniq -c >  ~/data/count/{{logical_date.strftime('%y%m%d')}}/count.log
                cat  ~/data/count/{{logical_date.strftime('%y%m%d')}}/count.log
            """
    )

    task_err = BashOperator(
            task_id="err.report",
            bash_command="""
              echo "err report"  
            """,
            trigger_rule="one_failed"
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make done"
                mkdir -p ~/data/done/{{logical_date.strftime('%y%m%d')}}
                touch ~/data/done/{{logical_date.strftime('%y%m%d')}}/_DONE
            """
    )

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')

    task_start >> task_date 
    task_date >> task_copy

    task_copy >> task_err >> task_end    # fail flow
    task_copy >> task_cut >> task_sort >> task_count >> task_done >> task_end   # success flow
