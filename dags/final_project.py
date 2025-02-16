# Import a few libraries
from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# Default arguments dictionary to pass into the dag below
default_args = {
    'owner': 'Sparkify_Data_Engineers',
    'start_date': pendulum.now(),
    'depends_on_past': False,               # No dependencies of past runs
    'retries': 3,                           # Tasks are retried three times on failure
    'retry_delay': timedelta(minutes=5),    # Retries occur every five minutes
    'catchup': False,                       # Catchup is turned off
    'email_on_retry': False                 # No email on retry         
}

# Create a new dag using the final_project function. Use the @dag decorator to 
# pass in the default arguments, a description, and a schedule. 
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'  # Run the dag every hour at minute zero
)
def final_project():

    # This operator doesn't do anything other than looking nice on a graph by marking the start
    start_operator = DummyOperator(task_id='Begin_execution')

    # Drop and recreate the tables in Redshift before we begin loading data
    create_tables = CreateTablesOperator(
        task_id="Create_tables",
        redshift_conn_id="redshift",
        sql_file_path="/opt/airflow/plugins/helpers/create_tables.sql"
    )

    # Create a string to pass to the stage_events_to_redshift task.  The files in the S3 bucket
    # are organized by date.  Use the execution date to pick up today's data.
    #s3_log_key = "log-data/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}/{{ execution_date.strftime('%Y-%m-%d') }}-events.json"
    
    # The S3 bucket only has data from November 2018. To see this actually load data,
    # you can uncomment the line below to override the dynamically created string above.
    s3_log_key = "log-data/2018/11/2018-11-01-events.json"

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-dend",
        s3_key=s3_log_key,
        json_path="s3://udacity-dend/log_json_path.json"
    )

    # Create a string to pass to the stage_songs_to_redshift task.  The files in the S3 bucket
    # are organized into alphabetical subfolders. The variable below allows the user to control
    # how many of these are loaded into the Redshift database.

    # To load ALL song data (may be slow/costly)
    #s3_song_key = "song-data/"  

    # To load only a subset of songs (faster).  Change letters to load other folders if desired.
    #s3_song_key = "song-data/A/A/A/"

    # To load just one file (fastest/least expensive)
    s3_song_key = "song-data/A/A/A/TRAAAAK128F9318786.json"

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key=s3_song_key,
        json_path="auto"
    )

    # Use the custom operator created in load_fact.py to load the Fact table.
    # The truncate_and_reload variable can be set to True if you want to remove
    # all rows from the table and start over with an empty table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert,
        truncate_and_reload=False
    )

    # The next four tasks are conceptually similar and use the same operators. They
    # pull a SELECT query from the SqlQueries class and pass that into the custom
    # operator created in load_dimensions.py.  That operator writes an insert statement
    # using the text of that query and passes it to Redshift.  The truncate_and_reload
    # variable makes it possible to switch between append-only and delete-load functionality
    # as required by the course rubric.
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate_and_reload=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        truncate_and_reload=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate_and_reload=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table='"time"',
        sql_query=SqlQueries.time_table_insert,
        truncate_and_reload=True
    )

    # This task runs data quality checks on the dimension and fact tables that were just loaded above.
    # The 'tests' list is a parameter that is passed to the operator created in data_quality.py.  To see
    # these tests fail, change the expected results of either query below to something greater than zero.
    # The test will fail prompting a retry.  After 3 retries (4 total attempts), airflow will mark the task
    # as a failure.
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        # These two test check for possible problems. The first verifies that all rows in the users
        # table have a userid.  The second checks for future-dated rows in the songplays table.  Both
        # should return zero records.  Other tests can be added if desired.
        tests=[
            {"sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL;", "expected_result": 0},
            {"sql": "SELECT COUNT(*) FROM songplays WHERE start_time::DATE > CURRENT_DATE;", "expected_result": 0}
        ]
    )

    # This operator doesn't do anything other than looking nice on a graph by marking the end
    end_operator = DummyOperator(task_id='Stop_execution')

    # Configure dependencies to match the flow depicted in the image in the instructions
    start_operator >> create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_user_dimension_table, load_time_dimension_table]
    load_artist_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

# Instantiate the dag.  Required for Airflow to run the dag.
final_project_dag = final_project()
