import airflow
from airflow import DAG, utils

from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default_args dictionary to set the default parameters of the DAG
default_args = {
    "owner": "airflow",
    "start_date": utils.dates.days_ago(0),
    "retries": 0,
    "catchup": False,
}

dag = DAG(
    "kym_scraping_dag",
    default_args=default_args,
    schedule_interval=None,
)

env = {
    "MONGO_URL": "mongodb://localhost:27017",
    "MONGO_DB": "airflow",
    "MONGO_COLLECTION": "memes",
    "REDIS_URL": "redis://localhost:6379/",
    "REDIS_PORT": 6379,
    "POSTGRES_USER": "airflow",
    "POSTGRES_PASSWORD": "airflow",
    "POSTGRES_DB": "airflow",
    "POSTGRES_HOST": "localhost",
}


feed_to_redis = DockerOperator(
    task_id="feed_to_redis",
    api_version="1.37",
    docker_url="TCP://docker-socket-proxy:2375",
    command="scrapy feed_memes kym_memes_choice.txt",  # To complete !
    image="kym-scraper",
    network_mode="host",
    environment=env,
    dag=dag,
)

scraping = DockerOperator(
    task_id="scraping",
    api_version="1.37",
    docker_url="TCP://docker-socket-proxy:2375",
    command="scrapy crawl memes -o kym_memes_choice.json",  # To complete !
    image="kym-scraper",
    network_mode="host",
    environment=env,
    dag=dag,
)

feed_to_redis >> scraping  