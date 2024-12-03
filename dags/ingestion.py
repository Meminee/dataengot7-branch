from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator
from airflow.sensors.filesystem import FileSensor
import airflow.utils as utils
from airflow import DAG

default_args = {
    'start_date': utils.dates.days_ago(0),
    'concurrency': 1,
    'retries': 0
}

# Use file watcher to watch file, if changed import in Mongo
with DAG('ingestion', default_args=default_args, schedule_interval=None) as dag:

    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath='/opt/airflow/data/raw.json',
        fs_conn_id='fs_default',
        poke_interval=30,
    )

    insert_json_file_task = MongoDBInsertJSONFileOperator(
        task_id='insert_json_file_into_mongodb',
        mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
        database='airflow',
        collection='raw_memes',
        filepath='/opt/airflow/data/raw.json',
    )

    file_sensor >> insert_json_file_task