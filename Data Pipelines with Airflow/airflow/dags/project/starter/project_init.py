import pendulum
import logging

from airflow.decorators import dag, task
from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from udacity.common import final_project_init_sql

@dag(
    start_date=pendulum.now()
)
def project_init():


    @task
    def load_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(final_project_init_sql.DROP_ALL_TABLES.format(
            aws_connection.login, aws_connection.password)
        )

    create_table_stagging_events=PostgresOperator(
       task_id="create_table_stagging_events",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_STAGING_EVENTS
    )

    create_table_stagging_songs = PostgresOperator(
        task_id="create_table_stagging_songs",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_STAGING_SONGS
    )

    create_table_songsplay = PostgresOperator(
        task_id="create_table_songsplay",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_SONGS_PLAY
    )

    create_table_dim_users= PostgresOperator(
        task_id="create_table_dim_users",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_DIM_USERS
    )

    create_table_dim_songs= PostgresOperator(
        task_id="create_table_dim_songs",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_DIM_SONGS
    )

    create_table_dim_time= PostgresOperator(
        task_id="create_table_dim_time",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_DIM_TIME
    )

    create_table_dim_artists= PostgresOperator(
        task_id="create_table_dim_artists",
        postgres_conn_id="redshift",
        sql=final_project_init_sql.CREATE_DIM_ARTISTS
    )

    end_operator = DummyOperator(task_id='Terminate_execution')

    load_data = load_task()
    load_data >> [create_table_stagging_events, create_table_stagging_songs, create_table_songsplay, create_table_dim_users, \
        create_table_dim_songs, create_table_dim_artists, create_table_dim_time] >> end_operator
project_init_dag = project_init()
