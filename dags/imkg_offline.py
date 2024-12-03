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
imkg_offline = DAG(
    dag_id="imkg_offline",
    default_args=default_args,
    schedule_interval=None,
)

# Function to insert JSON data into MongoDB
def insert_json_into_mongo(json_file_path, mongo_uri, database, collection):
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)

    client = pymongo.MongoClient(mongo_uri)
    db = client[database]
    col = db[collection]
    # Insert each document from the JSON data
    for document in json_data:
        col.insert_one(document)

start_task = DummyOperator(task_id='start_task', dag=imkg_offline)

# Task to insert JSON data into MongoDB
insert_kym_into_mongodb = PythonOperator(
    task_id='insert_kym_into_mongodb',
    python_callable=insert_json_into_mongo,
    op_args=['/opt/airflow/notebooks/baseData/imkg.kym.json', 'mongodb://mongo:27017', 'imkg', 'kym'],
    dag=imkg_offline,
)

# Task to insert JSON data into MongoDB
insert_kym_into_imgflip = PythonOperator(
    task_id='insert_kym_into_imgflip',
    python_callable=insert_json_into_mongo,
    op_args=['/opt/airflow/notebooks/baseData/imkg.imgflip.json', 'mongodb://mongo:27017', 'imkg', 'imgflip'],
    dag=imkg_offline,
)

def save2json(filename, dump):
    # Créer les dossiers si nécessaires
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Ouvrir et écrire dans le fichier
    with open(filename, "w") as out_file:
        json.dump(dump, out_file, indent=6)


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

    collection_name = "imgflip"

    # Retreive dataset
    client = pymongo.MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    data = list(collection.find())

    for document in data:
        if "_id" in document:
            document.pop("_id")

    save2json("/opt/airflow/notebooks/data/raw_imgflip_data.json", data)

    client.close()


load_data_from_Mongo = PythonOperator(
    task_id="load_data_from_Mongo",
    dag=imkg_offline,
    python_callable=extractFromMongo,
    trigger_rule="all_success",
)

notebook_cleaning = PapermillOperator(
    task_id="notebook_cleaning",
    input_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    output_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_extract_seed = PapermillOperator(
    task_id="notebook_extract_seed",
    input_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_extract_sibling = PapermillOperator(
    task_id="notebook_extract_sibling",
    input_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_extract_parent = PapermillOperator(
    task_id="notebook_extract_parent",
    input_nb="/opt/airflow/notebooks/Extract_Parent.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Parent.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_extract_children = PapermillOperator(
    task_id="notebook_extract_children",
    input_nb="/opt/airflow/notebooks/Extract_Children.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Children.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_extract_taxonomy = PapermillOperator(
    task_id="notebook_extract_taxonomy",
    input_nb="/opt/airflow/notebooks/Extract_Taxonomy.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Taxonomy.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

join_tasks_extracts = DummyOperator(
    task_id="join_tasks_extracts", dag=imkg_offline, trigger_rule="none_failed"
)

notebook_enrich_text = PapermillOperator(
    task_id="notebook_enrich_text",
    input_nb="/opt/airflow/notebooks/Enrich_Text.ipynb",
    output_nb="/opt/airflow/notebooks/Enrich_Text.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

notebook_enrich_tags = PapermillOperator(
    task_id="notebook_enrich_tags",
    input_nb="/opt/airflow/notebooks/Enrich_Tags.ipynb",
    output_nb="/opt/airflow/notebooks/Enrich_Tags.ipynb",
    dag=imkg_offline,
    trigger_rule="all_success",
)

join_tasks_enrich = DummyOperator(
    task_id="join_tasks_enrich", dag=imkg_offline, trigger_rule="none_failed"
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
    dag=imkg_offline,
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
    dag=imkg_offline,
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
    dag=imkg_offline,
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
    dag=imkg_offline,
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
    dag=imkg_offline,
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
    dag=imkg_offline,
)

generate_turtle_files_imgflip = DockerOperator(
    task_id="generate_turtle_files_imgflip",
    api_version="auto",  # You can set the Docker API version if needed
    docker_url="TCP://docker-socket-proxy:2375",
    command="-i /data/mappings/imgflip.yaml -o /data/mappings/imgflip.yaml.ttl",
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
    dag=imkg_offline,
)

join_docker_turtle = DummyOperator(
    task_id="join_docker_turtle", dag=imkg_offline, trigger_rule="none_failed"
)

create_nt_files_media_frame = BashOperator(
    task_id='create_nt_files_media_frame',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.media.frames.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.media.frames.nt",
)

create_nt_files_parent = BashOperator(
    task_id='create_nt_files_parent',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.parent.media.frames.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.parent.media.frames.nt",
)

create_nt_files_children = BashOperator(
    task_id='create_nt_files_children',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.children.media.frames.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.children.media.frames.nt",
)

create_nt_files_siblings = BashOperator(
    task_id='create_nt_files_siblings',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.siblings.media.frames.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.siblings.frames.nt",
)

create_nt_files_types = BashOperator(
    task_id='create_nt_files_types',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.types.media.frames.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.types.media.frames.nt",
)

create_nt_files_textual_enrich = BashOperator(
    task_id='create_nt_files_textual_enrich',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/kym.media.frames.textual.enrichment.yaml.ttl -o /opt/airflow/mappings/rdf/full/kym.textual.enrichment.media.frames.nt",
)

create_nt_files_imgflip = BashOperator(
    task_id='create_nt_files_imgflip',
    dag=imkg_offline,
    bash_command="java -jar /opt/airflow/mappings/mapper.jar -m /opt/airflow/mappings/imgflip.yaml.ttl -o /opt/airflow/mappings/rdf/full/imgflip.nt",
)

join_nt_files = DummyOperator(
    task_id="join_nt_files", dag=imkg_offline, trigger_rule="none_failed"
)

end = DummyOperator(task_id="end", dag=imkg_offline, trigger_rule="none_failed")

start_task >> insert_kym_into_mongodb >> insert_kym_into_imgflip >> load_data_from_Mongo
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
        generate_turtle_files_imgflip,
    ]
    >> join_docker_turtle
)
(
    join_docker_turtle
    >> [
        create_nt_files_media_frame,
        create_nt_files_parent,
        create_nt_files_children,
        create_nt_files_siblings,
        create_nt_files_types,
        create_nt_files_textual_enrich,
        create_nt_files_imgflip,
    ]
    >> join_nt_files
)
join_nt_files >> end
