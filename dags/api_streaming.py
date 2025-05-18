# streaming the API data to apache kafka
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
import logging

# default arguments 
default_args = {
    "owner": "Adam Worede",
    "start_date": datetime(2025, 5, 13)
}


def data_stream():

    # need to connect to the kafka queue to create a topic
    # sending a single data point to the producer
    producer = KafkaProducer(
        # connecting to the Kafka broker on the localhost mounted to the docker container
        # change localhost to broker once the docker container for the kafka broker is running
        bootstrap_servers=['broker:29092'],
        # max timeout
        max_block_ms=5000
    )
    current_time = time.time()
    while True:
        if time.time() > current_time + 60:
            break
        else:
            try:
                # extracting the data
                response = requests.get('https://randomuser.me/api/')
                # transforming the data
                transformed_data = transform_data(response.json()['results'][0])
                # push the data to the queue
                # a message will be sent to the kafka broker
                producer.send(
                    'users_created',
                    json.dumps(transformed_data).encode('utf-8')
                )
            
            except Exception as e:
                logging.error(f"Error: {e}")
                continue


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

