from scrapy_redis.spiders import RedisSpider


class BootstrapSpider(RedisSpider):
    name = "bootstrap"
    allowed_domains = ["knowyourmeme.com"]
    redis_key = "kym_bootstrap_queue:start_urls"
    redis_batch_size = 5

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 5

    def parse(self, response):
        """
        Parse the popular memes pages.
        """
        # for entry in response.css(".entry_list td"):
        #     url = entry.css("h2 a::attr(href)").get()
        #     if not url:
        #         continue  # Skip empty entries

        #     # Add the meme to the redis queue
        #     self.server.lpush("kym_memes_queue:start_urls", response.urljoin(url))

        print("DEBUG: Parsing response:", response.url)
        entries = response.css(".groups a")
        print(f"DEBUG: Found {len(entries)} entries.")
        
        for entry in entries:
            url = entry.css("::attr(href)").get()
            if url:
                # Ajouter à la file Redis
                self.server.lpush("kym_memes_queue:start_urls", response.urljoin(url))

