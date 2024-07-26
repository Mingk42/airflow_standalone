from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    'import_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='import DB from csv',
    schedule = "10 1 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['import', 'db', 'etl', 'shop'],
) as dag:



    task_check = BashOperator(
        task_id='check.done',
        bash_command="""
            DONE_PATH={{var.value.DONE_PATH}}/{{ds_nodash}}
            bash {{var.value.CHECK_SH}} $DONE_PATH
        """
    )

    task_to_csv = BashOperator(
        task_id = "to.csv",
        bash_command = """
            echo "to.csv"
            COUNT_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}

            mkdir -p $CSV_PATH
            cat  ${COUNT_PATH} | head -n -5  | awk '{print "^{{ds}}^,^" $2 "^,^" $1 "^"}' > ${CSV_PATH}/count.csv

            echo $CSV_PATH
        """
    )

    task_create_table = BashOperator(
            task_id="create.table",
            bash_command="""
                SQL={{var.value.SQL_PATH}}
                MYSQL_PWD={{var.value.DB_PASSWD}} mysql -u root < $SQL/create_db_table.sql
            """
    )

    task_to_tmp = BashOperator(
            task_id="to.tmp",
            bash_command="""
                echo "to.tmp"
                CSV_PATH=/home/root2/data/csv/{{ds_nodash}}/count.csv

                MYSQL_PWD={{var.value.DB_PASSWD}} mysql -u root -e "DELETE FROM history_db.tmp_cmd_usage WHERE dt='{{ds}}';"

                MYSQL_PWD={{var.value.DB_PASSWD}} mysql --local-infile=1 -u root history_db <<QUERY
                    LOAD DATA LOCAL INFILE '$CSV_PATH'
                    INTO TABLE tmp_cmd_usage
                    CHARACTER SET euckr
                    FIELDS 
                        TERMINATED BY ',' 
                        ENCLOSED BY '^'
                        ESCAPED BY '\b'
                    LINES TERMINATED BY '\n'
QUERY
            """
    )
    task_to_base = BashOperator(
            task_id="to.base",
            bash_command = """
                echo "to.base"

                MYSQL_PWD={{var.value.DB_PASSWD}} mysql -u root history_db <<EOF
                    DELETE FROM cmd_usage WHERE dt='{{ds}}'; 

                    INSERT INTO cmd_usage 
                    SELECT 
                    	CASE WHEN dt LIKE '%-%-%' THEN STR_TO_DATE(dt, '%Y-%m-%d') 
                    	ELSE STR_TO_DATE('1970-01-01', '%Y-%m-%d') END dt,
                    	command,
                    	CASE WHEN cnt REGEXP '[0-9]+$' THEN CAST(cnt AS UNSIGNED)
                    	ELSE -1 END cnt,
                        '{{ds}}' tmp_dt
                    FROM tmp_cmd_usage
                    WHERE dt='{{ds}}';
EOF
            """,
            trigger_rule="all_success"
    )

    task_done = BashOperator(
            task_id="make.done",
            bash_command="""
                echo "make done"
                DONE_PATH={{ var.value.IMPORT_DONE_PATH  }}/{{ds_nodash}}

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

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check

    task_check >> task_err >> task_end    # fail flow
    task_to_tmp >> task_to_base >> task_done >> task_end   # success flow
    task_check >> task_to_csv >> task_create_table >> task_to_tmp
