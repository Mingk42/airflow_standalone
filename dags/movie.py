from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator


with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie api import',
    schedule = "10 4 * * *",
    start_date=datetime(2024, 7, 20),
    catchup=True,
    tags=['movie', 'etl', 'shop'],
) as dag:


    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

#    run_this = PythonOperator(task_id="print_the_context", python_callable=print_context)

    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("="*20)
        print(f"ds_nodash =>>> {kwargs['ds_nodash']}")
        print(f"kwargs_type =>>> {type(kwargs)}")
        print("="*20)

        from movie.api.call import get_key,save2df
        key=get_key()
        print(f"movie_api_key ::::: {key}")
        yyyymmdd=kwargs["ds_nodash"]
        df=save2df(yyyymmdd)
        print(df)

    #task_get_data = PythonOperator(
    #    task_id='get.data',
    #    python_callable=get_data
    #)
    
    task_get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.1/api"],
        system_site_packages=False,
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
    #task_start >> virtualenv_task >> task_end
