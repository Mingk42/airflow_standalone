from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import', 'db', 'etl', 'shop'],
) as dag:



    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
#            ~/airflow/dags/check.done.sh {{logical_date.strftime('%y%m%d')}}
            bash {{var.value.CHECK_SH}} {{logical_date.strftime('%y%m%d')}}
#            echo "check.done"
#            DONE_PATH=~/data/done/{{logical_date.strftime('%y%m%d')}}
#            DONE_PATH_FILE="$DONE_PATH/_DONE"
#            echo $DONE_PATH_FILE

#            if [ -e "$DONE_PATH_FILE" ]; then
#                figlet "Let's move on!"
#                exit 0
#            else
#                echo "I'll be back => $DONE_PATH_FILE"
#                exit 1
#            fi
        """
    )

    task_to_csv = BashOperator(
        task_id = "to.csv",
        bash_command = """
            echo "to.csv"
            COUNT_PATH=~/data/count/{{logical_date.strftime('%y%m%d')}}/count.log
            CSV_PATH=~/data/csv/{{logical_date.strftime('%y%m%d')}}

            mkdir -p $CSV_PATH
            cat  ${COUNT_PATH} | awk '{print "{{ds}},"$2","$1}' > ${CSV_PATH}/count.csv

            echo $CSV_PATH
        """
    )

    task_to_tmp = SQLExecuteQueryOperator(
            task_id="to.tmp",
            sql="""
                show databases;
            """,
            conn_id='root'
    )

    task_to_base = BashOperator(
            task_id="to.base",
            bash_command = """
                echo "to.base"
            """,
            trigger_rule="all_success"
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

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check

    task_check >> task_err >> task_end    # fail flow
    task_check >> task_to_csv >> task_to_tmp >> task_to_base >> task_done >> task_end   # success flow
