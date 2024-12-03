import airflow
from airflow.operators.docker_operator import DockerOperator

NUMBER_OF_PAGES = 5

# Define the default arguments for the DAG.
default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 0,
}

# Define the DAG for the Internet Meme Knowledge Graph (IMKG) project.
dag = airflow.DAG(
    dag_id="scrap_websites",
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
    dag=dag,
)

imgflip_scraper_bootstrap = DockerOperator(
    task_id="imgflip_scraper_bootstrap",
    image="imgflip-scraper",
    command=f"scrapy crawl bootstrap",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=imgflip_env,
    dag=dag,
)

imgflip_scraper = DockerOperator(
    task_id="imgflip_scraper",
    image="imgflip-scraper",
    command=f"scrapy crawl templates",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=imgflip_env,
    dag=dag,
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
    dag=dag,
)

kym_scraper_bootstrap = DockerOperator(
    task_id="kym_scraper_bootstrap",
    image="kym-scraper",
    command=f"scrapy crawl bootstrap",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=dag,
)

kym_scraper = DockerOperator(
    task_id="kym_scraper",
    image="kym-scraper",
    command=f"scrapy crawl memes",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=dag,
)



kym_sync_children = DockerOperator(
    task_id="kym_sync_children",
    image="kym-scraper",
    command=f"scrapy sync_children",
    docker_url="TCP://docker-socket-proxy:2375",
    network_mode="host",
    environment=kym_env,
    dag=dag,
)

# Both scraper tasks are independent of each other.
imgflip_redis_init >> imgflip_scraper_bootstrap >> imgflip_scraper
kym_redis_init >> kym_scraper_bootstrap >> kym_scraper >> kym_sync_children  
