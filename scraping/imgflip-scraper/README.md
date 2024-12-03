# imgflip_scraper
Scrapes memes from imgflip.com.

## Usage
```sh
# First feed pages to process to redis
$ scrapy feed 5 # Number of pages to process

# Bootstrap the template queue
$ scrapy crawl bootstrap

# Start the scraper
$ scrapy crawl templates
```