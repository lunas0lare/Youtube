#we use real credentials for integration test

import requests
import pytest

#integration test for api requests
def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("API_KEY")
    channel_handle = airflow_variable("CHANNEL_HANDLE")

    url =f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"Request to youtube API fail: {e}")

