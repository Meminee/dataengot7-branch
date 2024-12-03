import airflow
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

NUMBER_OF_PAGES = 1

# Define the default arguments for the DAG.
default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 0,
}

# Define the DAG for the Internet Meme Knowledge Graph (IMKG) project.
imkg = airflow.DAG(
    dag_id="imkg",
    default_args=default_args,
    schedule_interval=None,
)

# Define the tasks for Imgflip scraping.
imgflip_env = {
    "MONGO_DB": "imkg",
    "MONGO_COLLECTION": "imgflip",
}

imgflip_redis_init = DockerOperator(
    task_id="imgflip_redis_init",
    image="imgflip-scraper",
    command=f"scrapy feed {NUMBER_OF_PAGES}",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=imgflip_env,
    dag=imkg,
)

imgflip_scraper_bootstrap = DockerOperator(
    task_id="imgflip_scraper_bootstrap",
    image="imgflip-scraper",
    command=f"scrapy crawl bootstrap",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=imgflip_env,
    dag=imkg,
)

imgflip_scraper = DockerOperator(
    task_id="imgflip_scraper",
    image="imgflip-scraper",
    command=f"scrapy crawl templates",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=imgflip_env,
    dag=imkg,
)

# Define the tasks for KnowYourMeme scraping.
kym_env = {
    "MONGO_DB": "imkg",
    "MONGO_COLLECTION": "kym",
    "POSTGRES_DB": "airflow",
    "POSTGRES_USER": "airflow",
    "POSTGRES_PASSWORD": "airflow",
}

kym_redis_init = DockerOperator(
    task_id="kym_redis_init",
    image="kym-scraper",
    command=f"scrapy feed {NUMBER_OF_PAGES}",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=imkg,
)

kym_scraper_bootstrap = DockerOperator(
    task_id="kym_scraper_bootstrap",
    image="kym-scraper",
    command=f"scrapy crawl bootstrap",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=imkg,
)

kym_scraper = DockerOperator(
    task_id="kym_scraper",
    image="kym-scraper",
    command=f"scrapy crawl memes",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=imkg,
)

kym_sync_children = DockerOperator(
    task_id="kym_sync_children",
    image="kym-scraper",
    command=f"scrapy sync_children",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=imkg,
)

join_scrapping = DummyOperator(
    task_id="join_scrapping", dag=imkg, trigger_rule="none_failed"
)

end = DummyOperator(task_id="end", dag=imkg, trigger_rule="none_failed")


# Both scraper tasks are independent of each other.
imgflip_redis_init >> imgflip_scraper_bootstrap >> imgflip_scraper >> join_scrapping
kym_redis_init >> kym_scraper_bootstrap >> kym_scraper >> kym_sync_children >> join_scrapping
join_scrapping >> end
