# README.md

`KYM_Scraper` is a `scrapy` project that scrapes the Know Your Meme website for memes and their associated data.
The scrapper use a `Redis` database to store the URLs to be scraped and store the scraped data in a `MongoDB` database.
Relationships between memes (e.g. parent-child) are stored in a `PostgreSQL` database before updating the `MongoDB` documents.

## How to use ?
```sh
# Bootstrap the list of memes to scrap
$ python kym_scraper/utils/feed.py
$ scrapy crawl bootstrap

# Scrap memes pages and update the database
$ scrapy crawl memes

# Update the database with the relationships between memes
$ python kym_scraper/utils/relationships.py
```

> **Notes**: None of the scripts is cleaning the database before updating it. You have to do it manually.