# streaming the API data to apache kafka
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

# default arguments 
default_args = {
    "owner": "Adam Worede",
    "start_date": datetime(2025, 5, 13)
}


def data_stream():
    # extracting the data
    response = requests.get('https://randomuser.me/api/')
    # transforming the data
    return transform_data(response.json()['results'][0])


def transform_data(api_response):
    data = {}
    data['first_name'] = api_response['name']['first']
    data['last_name'] = api_response['name']['last']
    data['gender'] = api_response['gender']
    data['city'] = api_response['location']['city']
    data['email'] = api_response['email']
    data['username'] = api_response['login']['username']
    data['profile_picture'] = api_response['picture']['thumbnail']
    data['password_salt'] = api_response['login']['salt']
    data['password_hash'] = api_response['login']['sha256']
    return data


# DAG definition
with DAG(
    'api_streaming',
    default_args=default_args,
    schedule="@daily"
    ) as dag:

    streaming = PythonOperator (
        task_id="streaming_raw_data",
        python_callable=data_stream
    )


test_run = data_stream()
print(json.dumps(test_run, indent=3))