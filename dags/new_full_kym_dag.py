from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.bash_operator import BashOperator
import airflow.utils as utils
from airflow import DAG
import pandas as pd
import pymongo
import json, csv
from csv import writer, reader
import os
from docker.types import Mount

default_args = {"start_date": utils.dates.days_ago(0), "concurrency": 1, "retries": 0}

# Generate the DAG
new_full_kym_dag = DAG(
    dag_id="new_full_kym_dag",
    default_args=default_args,
    schedule_interval=None,
)

def save2json(filename, dump):
    out_file = open(filename, "w")
    json.dump(dump, out_file, indent=6)
    out_file.close()


def extractFromMongo():
    # MongoDB connection details
    mongo_uri = "mongodb://mongo:27017"
    database_name = "imkg"
    collection_name = "kym"

    # Retreive dataset
    client = pymongo.MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    data = list(collection.find())

    for document in data:
        if "_id" in document:
            document.pop("_id")

    save2json("/opt/airflow/notebooks/data/raw_data.json", data)

    client.close()


load_data_from_Mongo = PythonOperator(
    task_id="load_data_from_Mongo",
    dag=new_full_kym_dag,
    python_callable=extractFromMongo,
    trigger_rule="all_success",
)

notebook_cleaning = PapermillOperator(
    task_id="notebook_cleaning",
    input_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    output_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_seed = PapermillOperator(
    task_id="notebook_extract_seed",
    input_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_sibling = PapermillOperator(
    task_id="notebook_extract_sibling",
    input_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_parent = PapermillOperator(
    task_id="notebook_extract_parent",
    input_nb="/opt/airflow/notebooks/Extract_Parent.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Parent.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_children = PapermillOperator(
    task_id="notebook_extract_children",
    input_nb="/opt/airflow/notebooks/Extract_Children.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Children.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_taxonomy = PapermillOperator(
    task_id="notebook_extract_taxonomy",
    input_nb="/opt/airflow/notebooks/Extract_Taxonomy.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Taxonomy.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

join_tasks_extracts = DummyOperator(
    task_id="join_tasks_extracts", dag=new_full_kym_dag, trigger_rule="none_failed"
)

notebook_enrich_text = PapermillOperator(
    task_id="notebook_enrich_text",
    input_nb="/opt/airflow/notebooks/Enrich_Text.ipynb",
    output_nb="/opt/airflow/notebooks/Enrich_Text.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_enrich_tags = PapermillOperator(
    task_id="notebook_enrich_tags",
    input_nb="/opt/airflow/notebooks/Enrich_Tags.ipynb",
    output_nb="/opt/airflow/notebooks/Enrich_Tags.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

join_tasks_enrich = DummyOperator(
    task_id="join_tasks_enrich", dag=new_full_kym_dag, trigger_rule="none_failed"
)

generate_turtle_files_text_enrich = DockerOperator(
    task_id="generate_turtle_files_text_enrich",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.media.frames.textual.enrichment.yaml -o /data/mappings/kym.media.frames.textual.enrichment.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

generate_turtle_files_media_frames = DockerOperator(
    task_id="generate_turtle_files_media_frames",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.media.frames.yaml -o /data/mappings/kym.media.frames.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

generate_turtle_files_parent = DockerOperator(
    task_id="generate_turtle_files_parent",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.parent.media.frames.yaml -o /data/mappings/kym.parent.media.frames.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

generate_turtle_files_children = DockerOperator(
    task_id="generate_turtle_files_children",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.children.media.frames.yaml -o /data/mappings/kym.children.media.frames.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

generate_turtle_files_siblings = DockerOperator(
    task_id="generate_turtle_files_siblings",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.siblings.media.frames.yaml -o /data/mappings/kym.siblings.media.frames.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

generate_turtle_files_types = DockerOperator(
    task_id="generate_turtle_files_types",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/kym.types.media.frames.yaml -o /data/mappings/kym.types.media.frames.yaml.ttl",
    # command="cat /data/mappings/kym.media.frames.textual.enrichment.yaml",
    image="rmlio/yarrrml-parser:latest",  # Docker image name and tag
    network_mode="host",  # Put Airflow in the same Docker network as the container
    tty=True,
    mounts=[
        Mount(
            source="/home/wann/INSA/DataEn/PROJECTO/data-engineering-project",
            target="/data",
            type="bind",
        )
    ],
    dag=new_full_kym_dag,
)

join_docker_turtle = DummyOperator(
    task_id="join_docker_turtle", dag=new_full_kym_dag, trigger_rule="none_failed"
)

end = DummyOperator(task_id="end", dag=new_full_kym_dag, trigger_rule="none_failed")

load_data_from_Mongo >> notebook_cleaning >> notebook_extract_seed
(
    notebook_extract_seed
    >> [
        notebook_extract_sibling,
        notebook_extract_parent,
        notebook_extract_children,
        notebook_extract_taxonomy,
    ]
    >> join_tasks_extracts
)
join_tasks_extracts >> notebook_enrich_text >> notebook_enrich_tags >> join_tasks_enrich
(
    join_tasks_enrich
    >> [
        generate_turtle_files_text_enrich,
        generate_turtle_files_media_frames,
        generate_turtle_files_parent,
        generate_turtle_files_children,
        generate_turtle_files_siblings,
        generate_turtle_files_types,
    ]
    >> join_docker_turtle
)
join_docker_turtle >> end
