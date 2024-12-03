from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import airflow.utils as utils
from pymongo import MongoClient
import json

default_args = {"start_date": utils.dates.days_ago(0), "concurrency": 1, "retries": 0}

# Function to insert JSON data into MongoDB
def insert_json_into_mongo(json_file_path, mongo_uri, database, collection):
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    # Print the first document of the JSON data
    print("First document of JSON data:")
    if isinstance(json_data, list) and len(json_data) > 0:
        print(json_data[0])
    elif isinstance(json_data, dict):
        print(json_data)
    else:
        print("Invalid JSON data format.")

    client = MongoClient(mongo_uri)
    db = client[database]
    col = db[collection]
    # Insert each document from the JSON data
    for document in json_data:
        col.insert_one(document)

# DAG definition
dag = DAG(
    'insert_mongo_dag',
    default_args=default_args,
    description='DAG to insert JSON data into MongoDB',
    schedule_interval=None,  # You can set the schedule_interval as needed
)

start_task = DummyOperator(task_id='start_task', dag=dag)

# Task to insert JSON data into MongoDB
insert_kym_into_mongodb = PythonOperator(
    task_id='insert_into_mongodb',
    python_callable=insert_json_into_mongo,
    op_args=['/opt/airflow/notebooks/baseData/imkg.kym.json', 'mongodb://mongo:27017', 'imkg', 'kym'],
    dag=dag,
)

start_task >> insert_kym_into_mongodb