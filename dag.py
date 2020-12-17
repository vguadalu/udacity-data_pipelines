from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTablesOperator)
from helpers import SqlQueries

s3_bucket = 'udacity-dend'
songs_key = 'song_data/A/A/A'
events_key = 'log_data'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True,
    'catchup': False,
    'email_on_retry': False,
    'max_active_runs': 1
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables={"staging_events":SqlQueries.create_staging_events,
            "staging_songs":SqlQueries.create_staging_songs,
            "songplays":SqlQueries.create_songplays_table,
            "users":SqlQueries.create_users_table,
            "artists":SqlQueries.create_artist_table,
            "songs":SqlQueries.create_songs_table,
            "time":SqlQueries.create_time_table}
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=s3_bucket,
    s3_key=events_key,
    json_format='s3://udacity-dend/log_json_path.json',
    truncate=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket=s3_bucket,
    s3_key=songs_key,
    json_format='auto',
    truncate=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_select=SqlQueries.songplay_table_insert,
    table="songplays",
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_select=SqlQueries.user_table_insert,
    table="users",
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_select=SqlQueries.song_table_insert,
    table="songs",
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_select=SqlQueries.artist_table_insert,
    table="artists",
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_select=SqlQueries.time_table_insert,
    table="time",
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table_check={'users':'userid', 'artists':'artistid', 'songs':'songid', 
                 'time':'start_time'},
    expected_nulls={'userid':0, 'artistid':0, 'songid':0, 'start_time':0}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, 
                         load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_time_dimension_table, 
 load_song_dimension_table, load_artist_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator