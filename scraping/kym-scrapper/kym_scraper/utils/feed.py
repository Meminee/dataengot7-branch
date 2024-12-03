from scrapy.commands import ScrapyCommand
import redis


class Feed(ScrapyCommand):
    def syntax(self):
        return "<pages>"

    def short_desc(self):
        return "Feed a specified number of pages to a redis queue with sorting options."

    def run(self, args, opts):
        if len(args) != 1:
            raise ValueError("You must provide the number of pages to feed.")

        try:
            number_of_page = int(args[0])
        except ValueError:
            raise ValueError(
                "Invalid argument. Please provide a valid integer for the number of pages."
            )

        # Feed the pages to the redis queue
        print("Feeding pages to the redis queue...")
        client = redis.from_url(self.settings.get("REDIS_URL"))

        for page in range(1, number_of_page + 1):
            client.lpush(
                "kym_bootstrap_queue:start_urls",
                f"https://knowyourmeme.com/memes/popular/page/{page}",
            )

        print("Done!")


if __name__ == "__main__":
    Feed().execute()
