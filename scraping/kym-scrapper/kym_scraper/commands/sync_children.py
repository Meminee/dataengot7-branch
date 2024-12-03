from scrapy.commands import ScrapyCommand
from pymongo import MongoClient
from ..utils.helper import PostgresHelper


class SyncChildren(ScrapyCommand):
    def syntax(self):
        return ""

    def short_desc(self):
        return "Synchronize the children and siblings lists in the MongoDB database."

    def run(self, args, opts):
        postgres_helper = PostgresHelper(**self.settings.get("POSTGRES_SETTINGS"))

        # Create the children table if it doesn't exist
        print("Making sure the children table exists in the PostgreSQL database...")
        postgres_helper.execute(
            """
            CREATE TABLE IF NOT EXISTS children (
                parent VARCHAR(255),
                child VARCHAR(255)
            );"""
        )

        # Retrieve all parent/child tuples from the PostgreSQL database
        print("Retrieving all parent/child tuples from the PostgreSQL database...")
        tuples = postgres_helper.execute(
            """
            SELECT parent, child FROM children;
            """
        )

        # Close the PostgreSQL connection
        postgres_helper.close_connection()

        # Connect to mongodb
        print("Connecting to the MongoDB database...")
        client = MongoClient(self.settings.get("MONGO_SETTINGS")["url"])
        db = client[self.settings.get("MONGO_SETTINGS")["db"]]
        collection = db[self.settings.get("MONGO_SETTINGS")["collection"]]

        # Update MongoDB documents with children lists based on retrieved tuples
        print("Updating MongoDB documents with children lists...")
        for parent, child in tuples:
            # Update MongoDB documents here based on the parent and child values
            collection.update_one(
                {"url": parent},
                {
                    "$push": {"children": child},
                },
            )

        # For each document in the MongoDB collection, update the siblings list
        print("Updating MongoDB documents with siblings lists...")
        for document in collection.find():
            # If document don't have parent, skip it
            if "parent" not in document:
                continue
            # Update the siblings list here
            collection.update_many(
                {"parent": document["parent"]},
                {
                    "$addToSet": {"siblings": document["url"]},
                },
            )

        # Close the MongoDB connection
        client.close()

        print("Update completed successfully.")
