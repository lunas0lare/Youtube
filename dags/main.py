from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.youtube import get_content_video, get_playlist_id, get_video_id, save_to_json

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
