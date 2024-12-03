from scrapy_redis.spiders import RedisSpider


class BootstrapSpider(RedisSpider):
    name = "bootstrap"
    allowed_domains = ["imgflip.com"]
    redis_key = "imgflip_bootstrap_queue:start_urls"
    redis_batch_size = 5

    # Max idle time(in seconds) before the spider stops checking redis and shuts down
    max_idle_time = 5

    def parse(self, response):
        """
        Parse the popular template pages.
        """
        for entry in response.css(".mt-title a"):
            url = entry.css("::attr(href)").get()
            if not url:
                continue  # Skip empty entries

            # Add the template to the redis queue
            self.server.lpush(
                "imgflip_templates_queue:start_urls", response.urljoin(url)
            )
