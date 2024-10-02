from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries

default_args = {
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'catchup':False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    end_date= datetime(2018, 11, 2),
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='neidy-bucket',
        s3_key='log-data',
        json_file_name='log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='neidy-bucket',
        s3_key='song-data/A/B'
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        destination_table="dim_users",
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dimension_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        destination_table="dim_songs",
        append_only=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dimension_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        destination_table="dim_artists",
        append_only=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        destination_table="dim_time",
        append_only=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='run_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'dim_users', 'dim_songs', 'dim_artists', 'dim_time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
        load_songplays_table >> [load_song_dimension_table , load_time_dimension_table \
            ,load_artist_dimension_table, load_user_dimension_table] >> \
        run_quality_checks >> end_operator

final_project_dag = final_project()
