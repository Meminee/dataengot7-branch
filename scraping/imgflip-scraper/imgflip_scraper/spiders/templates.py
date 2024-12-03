import re

from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient

BATCH_SIZE = 50


class TemplatesSpider(RedisSpider):
    name = "templates"
    allowed_domains = ["imgflip.com"]
    redis_key = "imgflip_templates_queue:start_urls"
    redis_batch_size = 5

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 5

    # Import settings from project
    settings = get_project_settings()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_client = MongoClient(self.settings.get("MONGO_SETTINGS")["url"])
        self.mongo_db = self.mongo_client[self.settings.get("MONGO_SETTINGS")["db"]]
        self.mongo_collection = self.mongo_db[
            self.settings.get("MONGO_SETTINGS")["collection"]
        ]
        self.batch = list()

    def parse(self, response):
        """
        Parse a given template page.
        """
        template_title = response.css("h1::text").get().removeprefix(" › ")
        template_id = response.url.split("/")[-2]
        for entry in response.css(".base-unit"):

            def parse_metrics(metric_string):
                """
                Given a string such as "1,457 views, 6 upvotes, 34 comments",
                return a dictionary such as {"view_count": 1457, "upvote_count": 6, "comment_count": 34}.
                """
                pattern = r"(\d+)\s+(\w+)"
                matches = re.findall(pattern, metric_string)

                metrics_dict = {}
                for value, metric_type in matches:
                    metric_type = metric_type.removesuffix("s")
                    metrics_dict[f"{metric_type}_count"] = int(value)

                return metrics_dict

            instance = {
                "template_title": template_title,
                "template_ID": template_id,
                "url": response.urljoin(entry.css("h2 a::attr(href)").get()),
                "title": entry.css("h2 a::text").get(),
                "author": entry.css(".u-username::text").get(),
                "image_url": response.urljoin(entry.css(".base-img::attr(src)").get()),
                "alt_text": entry.css(".base-img::attr(alt)").get(),
            }

            # Add metrics such as view_count, upvote_count, and comment_count
            view_info = entry.css(".base-view-count::text").get()
            instance.update(parse_metrics(view_info))

            # Yield the instance
            yield instance

        # Add document to batch
        self.batch.append(instance)

        # If the batch is full, insert it into the database
        if len(self.batch) >= BATCH_SIZE:
            self.mongo_collection.insert_many(self.batch)
            self.batch = list()

        # Follow the next page
        next_page = response.css(".pager-next::attr(href)").get()
        if next_page:
            yield response.follow(next_page)

    def closed(self, reason):
        # Insert the remaining documents in the batch
        if self.batch:
            self.mongo_collection.insert_many(self.batch)
            self.batch = list()
