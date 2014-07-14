# coding=utf-8
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
from spiders.weather_spider import WeatherSpider

__author__ = 'zephyre'


def setup_spider():
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelcrawlerPipeline': 800})
    crawler.configure()

    spider = WeatherSpider()
    crawler.crawl(spider)
    crawler.start()

    return spider


def main():
    setup_spider()
    reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    main()