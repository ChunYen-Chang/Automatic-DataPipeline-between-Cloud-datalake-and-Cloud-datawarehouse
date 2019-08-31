# import necessary packages
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# define the default argument for DAG
default_args = {
    'owner': 'ChunYen-Chang',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

# instantiate an DAG object
dag = DAG('Sparkify_data_pipeline_dag',
          default_args=default_args,
          description='Load event and song data from S3, transform data, and save transformed data in Redshift',
          schedule_interval='0 * * * *',
          catchup=False,
          max_active_runs=1
        )

# define start_operator task
start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

# define stage_events_to_redshift task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    create_table_sql=SqlQueries.staging_events_table_create,
    s3_bucket='udacity-dend',
    s3_key='log_data',
)

# define stage_songs_to_redshift task
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    create_table_sql=SqlQueries.staging_songs_table_create,
    s3_bucket='udacity-dend',
    s3_key='song_data',
)

# define load_songplays_table task
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    create_table_sql=SqlQueries.songplays_table_create,
    insert_table_sql=SqlQueries.songplay_table_insert,
    mode='append' 
)

# define load_user_dimension_table task
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    create_table_sql=SqlQueries.users_table_create,
    insert_table_sql=SqlQueries.user_table_insert,
    mode='overwrite'       
)

# define load_song_dimension_table task
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    create_table_sql=SqlQueries.songs_table_create,
    insert_table_sql=SqlQueries.song_table_insert,
    mode='overwrite'  
)

# define load_artist_dimension_table task
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    create_table_sql=SqlQueries.artists_table_create,
    insert_table_sql=SqlQueries.artist_table_insert,
    mode='overwrite'  
)

# define load_time_dimension_table task
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    create_table_sql=SqlQueries.time_table_create,
    insert_table_sql=SqlQueries.time_table_insert,
    mode='overwrite'  
)

# define run_quality_checks_songs task
run_quality_checks_songs = DataQualityOperator(
    task_id='Data_quality_checks_songs',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs'
)

# define run_quality_checks_users task
run_quality_checks_users = DataQualityOperator(
    task_id='Data_quality_checks_users',
    dag=dag,
    redshift_conn_id='redshift',
    table='users'
)

# define run_quality_checks_artists task
run_quality_checks_artists = DataQualityOperator(
    task_id='Data_quality_checks_artists',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists'
)

# define run_quality_checks_time task
run_quality_checks_time = DataQualityOperator(
    task_id='Data_quality_checks_time',
    dag=dag,
    redshift_conn_id='redshift',
    table='time'
)

# define end_operator task
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# define the task flow
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
load_user_dimension_table >> run_quality_checks_users
load_song_dimension_table >> run_quality_checks_songs
load_artist_dimension_table >> run_quality_checks_artists
load_time_dimension_table >> run_quality_checks_time
[run_quality_checks_users, run_quality_checks_songs, run_quality_checks_artists, run_quality_checks_time] >> end_operator