from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import airflow.utils as utils

from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator

default_args = {
    'start_date': utils.dates.days_ago(0),
    'concurrency': 1,
    'retries': 0
}

dag = DAG('mongo_test', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(task_id='start_task', dag=dag)

insert_task = MongoDBInsertOperator(
    task_id='insert_into_mongodb',
    mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
    database='airflow',
    collection='tests',
    document={'key': 'value', 'timestamp': "now"},
    dag=dag,
)

insert_json_file_task = MongoDBInsertJSONFileOperator(
    task_id='insert_json_file_into_mongodb',
    mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
    database='airflow',
    collection='tests',
    filepath='/opt/airflow/data/test.json',
    dag=dag,
)

start_task >> (insert_task, insert_json_file_task)