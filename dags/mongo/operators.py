import json

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator

class MongoDBInsertOperator(BaseOperator):
    """
    Custom Airflow operator to insert a document into a MongoDB collection using the MongoDocumentOperator.
    """
    @apply_defaults
    def __init__(self, mongo_conn_id, database, collection, document, *args, **kwargs):
        super(MongoDBInsertOperator, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.document = document

    def execute(self, context):
        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]
            collection.insert_one(self.document)
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)

class MongoDBInsertJSONFileOperator(BaseOperator):
    """
    Custom Airflow operator to insert a JSON file into a MongoDB collection using the MongoDocumentOperator.
    """

    @apply_defaults
    def __init__(self, mongo_conn_id, database, collection, filepath, *args, **kwargs):
        super(MongoDBInsertJSONFileOperator, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.filepath = filepath

    def execute(self, context):
        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]
            with open(self.filepath) as f:
                file_data = json.load(f)
            collection.insert_many(file_data)
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)