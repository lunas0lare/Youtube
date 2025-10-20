#this file is to create the DAG and the tasks for airflow to run

from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.youtube import get_content_video, get_playlist_id, get_video_id, save_to_json

from datawarehouse.dwh import core_table, staging_table
from dataquality.soda import yt_elt_data_quality
local_tz = pendulum.timezone("America/Los_Angeles")

default_args = {
    'owner' : 'lunas0lare',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'phamducduy102827@gmail.com',
    'max_active_runs':1,
    'dagrun_timeout': timedelta(hours = 1),
    'start_date': datetime(2025, 10, 13, tzinfo=local_tz)
}

staging_schema = "staging"
core_schema = "core"
with DAG(
    dag_id='produce_Json',
    default_args = default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',#run at 2pm,
    catchup=False#don't catch up form previous data 
) as dag:
    #define tasks
    playlist_id = get_playlist_id()
    video_id = get_video_id(playlist_id)
    extract_data = get_content_video(video_id)
    save_to_json_task = save_to_json(extract_data)

     #define dependencies
    playlist_id >> video_id >> extract_data >> save_to_json_task



with DAG(
    dag_id='update_db',
    default_args = default_args,
    description='DAG to process JSON file and insert data into both stagin and core schemas',
    schedule='0 15 * * *',#run at 2pm,
    catchup=False#don't catch up form previous data 
) as dag:
    #define tasks
    update_staging = staging_table()
    update_core = core_table()
     #define dependencies
    update_staging >> update_core

with DAG(
    dag_id='data_quality',
    default_args = default_args,
    description='run a data quality check on core and staging schema',
    schedule='0 16 * * *',#run at 2pm,
    catchup=False#don't catch up form previous data 
) as dag:
    #define tasks
    soda_validating_staging = yt_elt_data_quality(staging_schema)
    soda_validating_core = yt_elt_data_quality(core_schema)

    #set-up dependencies
    soda_validating_staging >> soda_validating_core

