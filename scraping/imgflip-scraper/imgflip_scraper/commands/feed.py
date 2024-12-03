from scrapy.commands import ScrapyCommand
import redis
import argparse
from urllib.parse import urlencode


class Feed(ScrapyCommand):
    def syntax(self):
        return "<pages> [--sort-by=<sort_option>]"

    def short_desc(self):
        return "Feed a specified number of pages to a redis queue with sorting options."

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_argument(
            "--sort-by",
            dest="sort_option",
            help="Sorting option for the meme templates (default: top-30-days)",
        )

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
            params = {"page": page}
            if opts.sort_option:
                params["sort"] = opts.sort_option
            url = f"https://imgflip.com/memetemplates?{urlencode(params)}"
            client.lpush("imgflip_bootstrap_queue:start_urls", url)

        print("Done!")


if __name__ == "__main__":
    Feed().execute()
