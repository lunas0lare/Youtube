import requests
import json
from datetime import date
import os
from dotenv import load_dotenv
from airflow.decorator import task
channels = "MrBeast"
max_result = 50
batch_size = 50
API_keys = "AIzaSyDA7iBNSNCmnWSJSLrTRUJOcoxtQ4yGlrs"
playlist_id = ""
url_channel = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channels}&key={API_keys}"

def read_file(file_name, output):

    with open(file_name, 'r') as f:
        for line in f:
            output.append(line[:-1])
        f.close()

def write_file(file_name, input):
    with open(file_name, 'r') as f:
        for i in range(0, len(input), 1):
            f.write(str(input[i]) + '\n')
        f.close()

@task
def get_playlist_id():
    try:
        response = requests.get(url_channel)
        data = response.json()

        response.raise_for_status()
        

        channel_items = data['items'][0]

        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise e
@task
def get_video_id(playlist_id):
    video_ids = []
    next_page_token = None
    url_video = f"https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_result}&playlistId={playlist_id}&key={API_keys}"
    try:
        while(True):
            temp_url  = url_video
            if next_page_token:
                 temp_url += f"&pageToken={next_page_token}"
                
            response = requests.get(temp_url)
            response.raise_for_status()
            data = response.json()

            #failsafe using [] if values are not in dict
            for item in data.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
                
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                break
            
        return video_ids  
    except requests.exceptions.RequestException as e:
        raise e

@task
def batch_video_ids(video_ids, batch_size):
    for video in range(0, len(video_ids), batch_size):
        # 'yield' returns this slice to the caller,
        # but instead of ending the function, it pauses here.
        # The function will resume from this line on the next iteration.
        # yield: 0->50; next call function: yield 51->100...
        yield video_ids[video : video + batch_size]

@task
def get_content_video(video_ids):
    
    try:  
        extracted_data = []
        for i in range(0, len(video_ids), batch_size):
            res = batch_video_ids(video_ids, batch_size)

            batch_videos = ','.join(next(res))
            # print(batch_videos)
            url_content_video = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={batch_videos}&key={API_keys}"
            
            response = requests.get(url_content_video)

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']

                video_data = {
                    "video_id": video_id,
                    "title": snippet['title'],
                    "publishedAt": snippet['publishedAt'],
                    "duration": contentDetails['duration'],
                    #some videos have 0 views or likes...
                    "viewCount": statistics.get('viewCount', None),
                    'likeCount': statistics.get('likeCount', None),
                    'commentCount': statistics.get('commentCount', None)
                }
            
                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_Data_{date.today()}.json"
    with open(file_path, 'w', encoding = 'utf-8') as f:
        json.dump(extracted_data, f, indent = 4, ensure_ascii = False)



if __name__ == "__main__":
    channel_playlist_id = get_playlist_id()
    video_ids = get_video_id(channel_playlist_id)
    video_data = get_content_video(video_ids)
    save_to_json(video_data)

a = (3, 4, 5)
a.append(1)