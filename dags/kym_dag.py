from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
import airflow.utils as utils
from airflow import DAG
import pandas as pd
import pymongo
import json, csv
from csv import writer, reader
import os

default_args = {"start_date": utils.dates.days_ago(0), "concurrency": 1, "retries": 0}

# Generate the DAG
kym_dag = DAG(
    dag_id="kym_dag",
    default_args=default_args,
    schedule_interval=None,
)

file_sensor = FileSensor(
    task_id="file_sensor",
    filepath="/opt/airflow/data/raw.json",
    fs_conn_id="fs_default",
    poke_interval=30,
    dag=kym_dag,
)

insert_json_file_task = MongoDBInsertJSONFileOperator(
    task_id="insert_json_file_into_mongodb",
    mongo_conn_id="mongodb_default",  # Connection ID for MongoDB
    database="memes",
    collection="raw_memes",
    filepath="/opt/airflow/data/raw.json",
    dag=kym_dag,
)


def save2json(filename, dump):
    out_file = open(filename, "w")
    json.dump(dump, out_file, indent=6)
    out_file.close()


def save2csv(filename, header, data):
    with open(filename, "w", newline="") as csvfile:
        csvw = csv.writer(
            csvfile, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )
        csvw.writerow(header)
        for d in data:
            csvw.writerow(d)


def extractFromMongo():
    # MongoDB connection details
    mongo_uri = "mongodb://mongo:27017"
    database_name = "memes"
    collection_name = "raw_memes"

    # Retreive dataset
    client = pymongo.MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    data = list(collection.find())

    for document in data:
        if "_id" in document:
            document.pop("_id")

    save2json("/opt/airflow/data/raw_data.json", data)

    client.close()


load_data_from_Mongo = PythonOperator(
    task_id="load_data_from_Mongo",
    dag=kym_dag,
    python_callable=extractFromMongo,
    trigger_rule="all_success",
)


def delete_file(filename):
    try:
        os.remove(filename)
        print(f"File '{filename}' has been deleted.")
    except FileNotFoundError:
        print(f"File '{filename}' not found and cannot be deleted.")
    except Exception as e:
        print(f"An error occurred while deleting the file '{filename}': {str(e)}")


def cleaning():
    raw_data = json.load(open("/opt/airflow/data/raw_data.json"))
    df_raw_data = pd.DataFrame(raw_data)
    delete_file("/opt/airflow/data/raw_data.json")

    raw_media_frames = []
    metas = []
    lds = []

    for index, row in df_raw_data.iterrows():
        m = row.to_dict()

        if (
            "sites" in m["url"]
            or "culture" in m["url"]
            or "subculture" in m["url"]
            or "event" in m["url"]
            or "people" in m["url"]
            or "type" in m["url"]
        ):
            continue
        if (
            "content" in m
            and "about" in m["content"]
            and "text" in m["content"]["about"]
        ):
            m["content"]["about"]["fulltext"] = "".join(m["content"]["about"]["text"])
        if (
            "content" in m
            and "origin" in m["content"]
            and "text" in m["content"]["origin"]
        ):
            m["content"]["origin"]["fulltext"] = "".join(m["content"]["origin"]["text"])
        if (
            "content" in m
            and "spread" in m["content"]
            and "text" in m["content"]["spread"]
        ):
            m["content"]["spread"]["fulltext"] = "".join(m["content"]["spread"]["text"])
        if (
            "content" in m
            and "subsection" in m["content"]
            and "text" in m["content"]["subsection"]
        ):
            m["content"]["subsection"]["fulltext"] = "".join(
                m["content"]["subsection"]["text"]
            )
        if "meta" in m:
            metas.append(m.pop("meta", None))
        if "ld" in m:
            lds.append(m.pop("ld", None))

        raw_media_frames.append(m)

    save2json("/opt/airflow/data/kym.media.frames.meta.json", metas)

    save2json("/opt/airflow/data/kym.media.frames.ls.json", lds)

    save2json("/opt/airflow/data/kym.media.frames.json", raw_media_frames)

    save2csv(
        "/opt/airflow/data/kym.media.frames.csv",
        ["title", "meme"],
        [[m["title"], m["url"]] for m in raw_media_frames],
    )


cleaning_raw_data = PythonOperator(
    task_id="cleaning_raw_data",
    dag=kym_dag,
    python_callable=cleaning,
    trigger_rule="all_success",
)


def extract_X(infile, outfile, x):
    xs = []
    for m in infile:
        if x in m.keys():
            if type(m[x]) == list:
                for s in m[x]:
                    xs = xs + [[m["url"], s]]
            else:
                xs = xs + [[m["url"], m[x]]]
    save2csv(outfile, ["meme", x], xs)


def extract_related_siblings():
    # raw_media_frames  = json.load(open('/opt/airflow/data/kym.media.frames.json'))
    seed_raw_media_frames = json.load(
        open("/opt/airflow/data/kym.seed.media.frames.json")
    )

    extract_X(
        seed_raw_media_frames,
        "/opt/airflow/data/kym.siblings.media.frames.csv",
        "siblings",
    )


extract_related_siblings = PythonOperator(
    task_id="extract_related_siblings",
    dag=kym_dag,
    python_callable=extract_related_siblings,
    trigger_rule="all_success",
)


def extract_related_parent():
    # raw_media_frames  = json.load(open('/opt/airflow/data/kym.media.frames.json'))
    seed_raw_media_frames = json.load(
        open("/opt/airflow/data/kym.seed.media.frames.json")
    )

    extract_X(seed_raw_media_frames, "data/kym.parent.media.frames.csv", "parent")


extract_related_parent = PythonOperator(
    task_id="extract_related_parent",
    dag=kym_dag,
    python_callable=extract_related_parent,
    trigger_rule="all_success",
)


def extract_related_children():
    # raw_media_frames  = json.load(open('/opt/airflow/data/kym.media.frames.json'))
    seed_raw_media_frames = json.load(
        open("/opt/airflow/data/kym.seed.media.frames.json")
    )

    extract_X(seed_raw_media_frames, "data/kym.children.media.frames.csv", "children")


extract_related_children = PythonOperator(
    task_id="extract_related_children",
    dag=kym_dag,
    python_callable=extract_related_children,
    trigger_rule="all_success",
)


def extract_types(infile, outfile):
    xs = []
    for m in infile:
        if "details" in m.keys() and "type" in m["details"].keys():
            for x in m["details"]["type"]:
                xs = xs + [[m["url"], x]]
    save2csv(outfile, ["meme", "type"], xs)


def extract_taxonomy():
    # raw_media_frames  = json.load(open('/opt/airflow/data/kym.media.frames.json'))
    seed_raw_media_frames = json.load(
        open("/opt/airflow/data/kym.seed.media.frames.json")
    )

    extract_types(seed_raw_media_frames, "data/kym.types.media.frames.csv")


extract_taxonomy = PythonOperator(
    task_id="extract_taxonomy",
    dag=kym_dag,
    python_callable=extract_taxonomy,
    trigger_rule="all_success",
)

join_tasks = DummyOperator(
    task_id="coalesce_transformations", dag=kym_dag, trigger_rule="none_failed"
)


def get_textual_entities(infile, entity_file, outfile):
    memes_entities = []
    cache = []
    errors = []
    with open(infile) as csvfile:
        urls = reader(csvfile)
        # Loading file with dbpedia entities
        with open(entity_file) as efile:
            data = json.load(efile)
            # Mapping DBPedia entities to Wikidata
            with open("data/dbpedia-wikidata.json") as jsonfile:
                db2wdmapping = json.load(jsonfile)
                for row in urls:
                    if row[1] in data.keys() and not row[1] in cache:
                        cache.append([row[1]])
                        newdata = data[row[1]]
                        newdata["url"] = row[1]
                        if "Resources" in newdata.keys():
                            for r in newdata["Resources"]:
                                res = r["@URI"].replace(
                                    "http://dbpedia.org/resource/", ""
                                )
                                if res in db2wdmapping.keys():
                                    r["QID"] = (
                                        "https://www.wikidata.org/wiki/"
                                        + db2wdmapping[res]
                                    )
                                if type(r["@types"]) == str:
                                    ls = (
                                        r["@types"]
                                        .replace(
                                            "DUL:",
                                            "http://www.loa-cnr.it/ontologies/DOLCE-Lite#",
                                        )
                                        .replace(
                                            "Wikidata:",
                                            "https://www.wikidata.org/wiki/",
                                        )
                                        .replace("Schema:", "https://schema.org/")
                                        .split(",")
                                    )
                                    ls = list(filter(None, ls))
                                    ls = [l for l in ls if "DBpedia" not in l]
                                # elif(type(r["@types"])==list):
                                # ls = list(map(lambda s: s.replace("DUL:", "http://www.loa-cnr.it/ontologies/DOLCE-Lite#").replace("Wikidata:", "https://www.wikidata.org/wiki/").replace("Schema:", "https://schema.org/").replace("DBpedia:", ""),r["@types"]))
                                # errors.append([row[1],res,r["@types"]])
                                # Removing dbpedia types that do not have a correspondence in wikidata
                                for l in ls:
                                    if l in db2wdmapping.keys():
                                        l = (
                                            "https://www.wikidata.org/wiki/"
                                            + db2wdmapping[l]
                                        )
                                r["@typeList"] = ls
                        memes_entities.append(data[row[1]])
    save2json(outfile, memes_entities)
    save2csv("errors.csv", ["kym", "entity", "@types"], errors)


def enrich_text_spotlight():
    spotlight_enrichment_raw = "data/kym.spotlight.raw.json"

    get_textual_entities(
        "data/kym.media.frames.csv",
        spotlight_enrichment_raw,
        "data/kym.media.frames.textual.enrichment.json",
    )


enrich_text_spotlight = PythonOperator(
    task_id="enrich_text_spotlight",
    dag=kym_dag,
    python_callable=enrich_text_spotlight,
    trigger_rule="all_success",
)


def enrich_text_tag_spotlight():
    spotlight_enrichment_raw = "data/kym.spotlight.raw.tags.json"

    get_textual_entities(
        "data/kym.media.frames.csv",
        spotlight_enrichment_raw,
        "data/kym.media.frames.tags.enrichment.json",
    )


enrich_text_tag_spotlight = PythonOperator(
    task_id="enrich_text_tag_spotlight",
    dag=kym_dag,
    python_callable=enrich_text_tag_spotlight,
    trigger_rule="all_success",
)

end = DummyOperator(task_id="end", dag=kym_dag, trigger_rule="none_failed")


docker_env = {
    "MONGO_URL": "mongodb://localhost:27017",
    "MONGO_DB": "memes",
    "MONGO_COLLECTION": "raw_memes",
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
    command="python kym_scraper/utils/feed.py",
    image="kym-scraper",
    network_mode="host",
    environment=docker_env,
    dag=kym_dag,
)

bootstrap = DockerOperator(
    task_id="bootstrap",
    api_version="1.37",
    docker_url="TCP://docker-socket-proxy:2375",
    command="scrapy crawl bootstrap",
    image="kym-scraper",
    network_mode="host",
    environment=docker_env,
    dag=kym_dag,
)

scraping = DockerOperator(
    task_id="scraping",
    api_version="1.37",
    docker_url="TCP://docker-socket-proxy:2375",
    command="scrapy crawl memes",
    image="kym-scraper",
    network_mode="host",
    environment=docker_env,
    dag=kym_dag,
)

feed_to_redis >> bootstrap >> scraping >> load_data_from_Mongo >> cleaning_raw_data
(
    cleaning_raw_data
    >> [
        extract_related_siblings,
        extract_related_parent,
        extract_related_children,
        extract_taxonomy,
    ]
    >> join_tasks
)
join_tasks >> enrich_text_spotlight
enrich_text_spotlight >> enrich_text_tag_spotlight >> end
