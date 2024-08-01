from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator


with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },
    # max_active_runs:1           #  여러 날짜를 몇 개나 병렬로 처리
    # max_active_tasks:3,         #  1개 날짜에 대해 병렬처리를 몇 개 지원
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

    def get_data(ds_nodash, **kwargs):
        print(ds_nodash)
        print(kwargs)
        print("="*20)
        print(f"ds_nodash =>>> {ds_nodash}")
        print(f"kwargs_type =>>> {type(kwargs)}")
        print("="*20)

        from movie.api.call import save2df
        # yyyymmdd=kwargs["ds_nodash"]
        # df=save2df(yyyymmdd)
        df=save2df(ds_nodash)
        print(df)

    #task_get_data = PythonOperator(
    #    task_id='get.data',
    #    python_callable=get_data
    #)
    
    task_get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
        system_site_packages=False,
        trigger_rule="none_failed",
        # venv_cache_path="/home/root2/tmp/airflow_venv/get_data"
    )

    def branch_func(ds_nodash):
        import os

        l_d = ds_nodash # kwargs["ds_nodash"]
        home_dir = os.path.expanduser("~")
        path = os.path.join(
                home_dir,
                "tmp",
                "test_parquet",
               f"load_dt={l_d}"
               )

        #if os.path.exists(f"{home_dir}/tmp/test_parquet/load_dt={l_d}"):
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.start", "task.echo"

    branch_op=BranchPythonOperator(
            task_id="branch.op",
            python_callable=branch_func
    )

    def save_data(ds_nodash):
        from movie.api.call import apply_type2df

        df=apply_type2df(load_dt=ds_nodash)
        print("*"*33)
        print(df.head(3))
        print("*"*33)
        print(df.dtypes)
        print("*"*33)

        g=df.groupby("openDt")
        sum_df=g.agg({"audiCnt":"sum"}).reset_index()
        print("===개봉일별 영화 관객수 합계===")
        print(sum_df)


    def common_get_data(ds_nodash,url_param):
        from movie.api.call import save2df
        
        df = save2df(dt=ds_nodash, url_param=url_param)
        print("-----------------")
        print(df)
        print("-----------------")
        
        for k,v in url_param.items():
            df[k]=v
        
        print(df)

        p_cols = ["load_dt"] + list(url_param.keys())
        # p_cols = list(url_param.keys()).insert(0,"load_dt")
        print("url param:::::::::",url_param,list(url_param.keys()))
        print("pcol:::::::::",p_cols)

        df.to_parquet("/home/root2/tmp/test_parquet/",
                      # partition_cols=['load_dt']
                      partition_cols=p_cols
                      )

    task_save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
        system_site_packages=False,
        trigger_rule="none_skipped",
    )

    multi_y = PythonVirtualenvOperator(     # 다양성 영화 여부
            task_id='multi.y',
            python_callable=common_get_data,
            # op_kwargs={"multiMovieYn":"Y"},
            op_kwargs={"url_param":{"multiMovieYn":"Y"}},
            requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
            system_site_packages=False,
    )
    multi_n = PythonVirtualenvOperator(     # 다양성 영화 여부
            task_id='multi.n',
            python_callable=common_get_data,
            # op_kwargs={"multiMovieYn":"N"},
            op_kwargs={"url_param":{"multiMovieYn":"N"}},
            requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
            system_site_packages=False,
    )
    nation_k = PythonVirtualenvOperator(     # 국산영화 여부
            task_id='nation.k',
            python_callable=common_get_data,
            # op_kwargs={"repNationCd":"K"},
            op_kwargs={"url_param":{"repNationCd":"K"}},
            requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
            system_site_packages=False,
    )
    nation_f = PythonVirtualenvOperator(     # 국산영화 여부
            task_id='nation.f',
            python_callable=common_get_data,
            # op_kwargs={"repNationCd":"F"},
            op_kwargs={"url_param":{"repNationCd":"F"}},
            requirements=["git+https://github.com/Mingk42/-Mingk42-movie.git@v0.2.5/api_test"],
            system_site_packages=False,
    )

    task_rm_dir = BashOperator(
            task_id="rm.dir",
            bash_command="""
                echo "rm.dir.start"
                rm -rf ~/tmp/test_parquet/load_dt={{ds_nodash}}
                echo "rm.dir.end"
            """
    )

    task_echo = BashOperator(
            task_id="task.echo",
            bash_command="""
                echo "echo.start"
                echo "echo.end"
            """
    )

    task_end = EmptyOperator(task_id='end', trigger_rule='all_done')
    task_start = EmptyOperator(task_id='start')


    task_get_start = EmptyOperator(task_id='get.start', trigger_rule="one_success") 
    task_get_end = EmptyOperator(task_id='get.end', trigger_rule="none_failed") 

    task_throw_err = BashOperator(
            task_id='throw.err',
            bash_command="""
                exit 1
            """,
            trigger_rule="all_done"
    )
    """
    task_start >> [task_throw_err, branch_op]
    branch_op >> [task_rm_dir, task_echo, task_get_start]
    [task_throw_err, task_echo, task_rm_dir] >> task_get_start
    task_get_start >> [multi_y, multi_n, nation_k, nation_f, task_get_data] >> task_get_end >> task_save_data >> task_end
    """

    task_start >> branch_op
    task_start >> task_throw_err >> task_save_data

    branch_op >> task_rm_dir >> task_get_start
    branch_op >> task_echo 
    branch_op >> task_get_start
    task_get_start >> [multi_y, multi_n, nation_k, nation_f,] >> task_get_end

    task_get_end >> task_save_data >> task_end
