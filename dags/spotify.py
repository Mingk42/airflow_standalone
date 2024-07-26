from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'spotify',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='spotify search result analysis DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['spotify'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='getSessionKey',
        depends_on_past=False,
        bash_command='curl -X POST "https://accounts.spotify.com/api/token" \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "grant_type=client_credentials&client_id=XXXXXX&client_secret=XXXXXX"',
        retries=3,
    )

    t3 = BashOperator(
            task_id='getData',
            bash_command="""
                curl "https://api.spotify.com/v1/artists/XXXXXX" -H "Authorization: Bearer  SessionKey" > ~/tmp/data.json
            """
     )

    t4 = BashOperator(
            task_id='getArtist',
            bash_command='cat ~/tmp/data.json | grep artist'
    )
    t5 = BashOperator(
            task_id='getCoverImg',
            bash_command='cat ~/tmp/data.json | grep img'
    )
    t6 = BashOperator(
            task_id='getAlbum',
            bash_command='cat ~/tmp/data.json | grep album'
    )
    t7 = BashOperator(
            task_id='getGenre',
            bash_command='cat ~/tmp/data.json | grep genre'
    )

    t8 = DummyOperator(task_id='SearchRst')
    t9 = DummyOperator(task_id='AnalysisLike')

    task_err=BashOperator(
            task_id="err.report",
            bash_command="""
                echo "error"
            """,
            trigger_rule="one_failed"
    )

    task_start = DummyOperator(task_id='start')
    task_end = DummyOperator(task_id='end', trigger_rule="all_done")
   
    t1 >> t2 >> t3 >> [t4 ,t5, t6, t7] >> t8
    [t4, t7] >> t9
    [t8, t9] >> task_end
    task_start >> t1
    [t4 ,t5, t6, t7] >> task_err >> task_end
