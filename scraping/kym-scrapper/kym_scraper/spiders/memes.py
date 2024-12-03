import json

from scrapy_redis.spiders import RedisSpider
from scrapy.utils.project import get_project_settings
from urllib.parse import unquote
from collections import defaultdict
from pymongo import MongoClient

from ..utils.helper import PostgresHelper


class MemesSpider(RedisSpider):
    """
    A Spider that scrapes a given meme page from Know Your Meme.
    This consumes the kym_memes_queue:start_urls queue from a local redis server.

    Usage:
        scrapy crawl memes -o memes.json
    """

    name = "memes"
    allowed_domains = ["knowyourmeme.com"]
    redis_key = "kym_memes_queue:start_urls"
    redis_batch_size = 5  # Fetch 5 urls at a time from redis

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 5

    # In-memory list of children to be inserted into the database
    children = []

    # Retreive settings from the project
    settings = get_project_settings()

    # Setup MongoDB connection

    mongo_client = MongoClient(settings.get("MONGO_SETTINGS")["url"])
    mongo_db = settings.get("MONGO_SETTINGS")["db"]
    mongo_collection = settings.get("MONGO_SETTINGS")["collection"]

    def parse(self, response):
        """
        Parse a meme page.
        The parsing is splitted into multiple functions for better readability.
        """

        # Parse the info, meta and schema data
        entry = self.parse_info(response)
        entry["meta"] = self.parse_meta(response)
        entry["ld"] = self.parse_schema(response)
        entry["details"] = self.parse_details(response)

        # If the page is a meme page, parse the body
        if entry["category"] == "Meme":
            entry["content"] = self.parse_content(response)

        # Insert the entry into the database
        self.mongo_client[self.mongo_db][self.mongo_collection].insert_one(entry)

    def close(self, reason):
        """
        Called when the spider is closed.
        """

        # Insert the children into the database
        postgres_helper = PostgresHelper(**self.settings.get("POSTGRES_SETTINGS"))
        postgres_helper.execute_many(
            """
            INSERT INTO children (parent, child) VALUES (%s, %s);
            """,
            self.children,
        )

        # Close the PostgreSQL connection
        postgres_helper.close_connection()

    def parse_content(self, response):
        body = response.css(".bodycopy")

        # The content is a dictionary of the following form:
        # Content -> Section -> [Subsection] -> List of items
        content = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        section, subsection = None, None

        # Go through all the nodes in the body, and classify them
        # by their type.
        for node in body.xpath("child::node()"):
            tag_name = node.xpath("name()").get()

            # If the node is a primary heading, it is a section
            if tag_name in {"h1", "h2", "h3"}:
                section = node.xpath("text()").get()
                if section:
                    section = section.strip().lower()
                subsection = None  # Reset the subsection

            # If the node is a secondary heading, it is a subsection
            elif tag_name in {"h4", "h5", "h6"}:
                subsection = node.xpath("text()").get()
                if subsection:
                    subsection = subsection.strip().lower()

            # If the node is an image, it is an image
            elif tag_name == "img":
                url = node.xpath("@data-src").get()
                content[section][subsection]["images"].append(url)

            # If the node is a paragraph, it is a paragraph
            elif tag_name == "p":
                text = node.xpath("normalize-space()").get().strip()
                content[section][subsection]["text"].append(text)

                # If the paragraph has a link, add it to the links
                for link in node.xpath(".//a"):
                    url = link.xpath("@href").get() or link.xpath("@hrf").get()
                    if not url or url.startswith("#"):
                        continue  # Skip empty and internal links

                    # Complete relative urls, don't touch absolute urls
                    url = response.urljoin(url)

                    # Add the link to the list of links
                    content[section][subsection]["links"].append(url)

        def move_none_children_to_parent(d):
            """
            Move children of 'None' key to the parent.
            """
            if isinstance(d, dict):
                for key, value in list(d.items()):
                    if key == None and isinstance(value, dict):
                        for inner_key, inner_value in value.items():
                            d[inner_key] = inner_value
                        del d[key]
                    else:
                        move_none_children_to_parent(value)

        # Move all the children of the 'None' key to the parent
        move_none_children_to_parent(content)

        return content

    def parse_info(self, response):
        """
        Parse the info from a meme page.
        """

        infos = {
            "title": response.css(".info h1::text").get().strip(),
            "url": response.url,
            "category": response.css(".entry-category-badge::text").get(),
            "template_image_url": response.css("img::attr(data-src)").get(),
        }

        # Get all the timestamps in the page
        timestamps = response.css(".timeago")
        infos |= {
            "last_update_source": timestamps[0].css("::attr(title)").get(),
            "added": timestamps[1].css("::attr(title)").get(),
        }

        # Get parent if it exists
        if parent := response.css(".parent"):
            parent_url = response.urljoin(parent.css("a::attr(href)").get())
            infos["parent"] = parent_url

            # Add parent to the queue
            self.server.lpush("kym_memes_queue:start_urls", parent_url)

            # Add the current page to the parent's children
            self.children.append((parent_url, response.url))

        # TODO Find a way to load the iframe with search interest data

        # Prepare both children and siblings fields
        infos["children"] = []
        infos["siblings"] = []

        return infos

    def parse_search_interest(self, response):
        return [
            item.css("::text").get()
            for item in response.css(".fe-line-chart-legend-text")
        ]

    def parse_meta(self, response):
        """
        Parse the meta data from a meme page.
        """
        meta = {
            "og:title": response.xpath("//meta[@property='og:title']/@content").get(),
            "og:image": response.xpath("//meta[@property='og:image']/@content").get(),
            "og:description": response.xpath(
                "//meta[@property='og:description']/@content"
            ).get(),
            "og:url": unquote(response.url),
        }

        # Add fields that are constant for all documents
        meta |= {
            "og:site_name": "Know Your Meme",
            "og:image:width": "600",
            "og:image:height": "315",
            "og:type": "article",
            "fb:app_id": "104675392961482",
            "fb:pages": "88519108736",
            "article:publisher": "https://www.facebook.com/knowyourmeme",
            "twitter:card": "summary_large_image",
            "twitter:site": "@knowyourmeme",
            "twitter:creator": "@knowyourmeme",
        }

        # Add fields that are duplicates of other fields
        meta |= {
            "twitter:title": meta["og:title"],  # Duplicate of og:title
            "twitter:description": meta[
                "og:description"
            ],  # Duplicate of og:description and description
            "twitter:image": meta["og:image"],  # Duplicate of og:image
            "description": meta[
                "og:description"
            ],  # Duplicate of og:description and twitter:description
        }

        # Return the meta data
        return meta

    def parse_schema(self, response):
        """
        Parse the schema data from a meme page.
        """
        # Select all the <script> tags with type 'application/ld+json'
        script_tags = response.xpath(
            '//script[@type="application/ld+json"]/text()'
        ).getall()

        # Parse the JSON content of each <script> tag
        data = []
        for script_content in script_tags:
            if not script_content:
                continue

            # Parse the JSON content
            data.append(json.loads(script_content))

        return data

    def parse_details(self, response):
        """
        Parse the details from a meme page.
        """
        details = {}

        fields = ["Origin", "Year", "Status", "Type"]
        for field in fields:
            if value := response.xpath(
                f'//dt[contains(text(),"{field}")]/following-sibling::*[1]/text()'
            ).get():
                details[field.lower()] = value.strip()
        return details
