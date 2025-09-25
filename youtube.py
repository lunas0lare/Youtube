import requests
import json
channels = "MrBeast"
API_keys = "AIzaSyDA7iBNSNCmnWSJSLrTRUJOcoxtQ4yGlrs"
url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channels}&key={API_keys}"


def get_playlist_id():
    try:
        response = requests.get(url, timeout = 1)
        data = response.json()

        response.raise_for_status()
        # print(json.dumps(data, indent = 4))

        channel_items = data['items'][0]

        channel_playlist_id = channel_items['contentDetails']['relatedPlaylists']['uploads']
        return channel_playlist_id

    except requests.exceptions.RequestException as e:
        raise e

if __name__ == "__main__":
    channel_playlist_id = get_playlist_id()
    print(channel_playlist_id)

