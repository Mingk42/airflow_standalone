from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint as pp

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
    
    REQUIREMENTS=["git+https://github.com/Mingk42/Mingk42_movie_agg.git@v0.5.0/agg"]
    
    def gen_empty(*ids):
        if len(ids)==1:
            return EmptyOperator(task_id=ids[0])
        task=[]
        for id in ids:
            task.append(EmptyOperator(task_id=id))
        return task

    def gen_vpython(id,**kwargs):

        return PythonVirtualenvOperator(
                task_id=id,
                python_callable=kwargs["callback"],
                requirements=REQUIREMENTS,
                system_site_packages=False,
                op_kwargs=
                   # "task_name":kwargs["op_kwargs"]
                   kwargs["op_kwargs"]
                
        )

    def pro_data(task_name, **params):
        print("*"*30)
        print(task_name)
        print(params)
        if "task_name" in params:
            print("----------------Y")
        else:
            print("----------------N")
        print("*"*30)

    def pro_merge(ds_nodash):
        from movie_agg.utils import merge

        df=merge(int(ds_nodash))
        print("x"*33)
        print(df)
        print("x"*33)

    # task_apply_type=EmptyOperator(task_id="apply.type")
    # task_merge_df=EmptyOperator(task_id="merge.df")
    # task_de_dup=EmptyOperator(task_id="de.dup")
    # task_summary_df=EmptyOperator(task_id="summary.df")

    task_apply_type=gen_vpython(id="apply.type",op_kwargs={"task_name":"apply.type"},callback=pro_data)
    task_merge_df=gen_vpython(id="merge.df",op_kwargs={"task_name":"merge.df"},callback=pro_merge)
    task_de_dup=gen_vpython(id="de.dup",op_kwargs={"task_name":"de.dup"},callback=pro_data)
    task_summary_df=gen_vpython(id="summary.df",op_kwargs={"task_name":"summary.df"},callback=pro_data)

    """
    task_apply_type=PythonVirtualenvOperator(task_id="apply.type")
    task_merge_df=PythonVirtualenvOperator(task_id="merge.df")
    task_de_dup=PythonVirtualenvOperator(task_id="de.dup")
    task_summary_df=PythonVirtualenvOperator(task_id="summary.df")
    """
    # task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    # task_start = EmptyOperator(task_id='start')
    task_start, task_end = gen_empty("start", "end")

    task_start >> task_merge_df >> task_de_dup >> task_apply_type >> task_summary_df >> task_end
