# coding=utf-8
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
# from spiders.weather_spider import WeatherSpider

import sys
# print sys.path
from spiders.travel_notes import TravelNote

sys.path.append('.')

from spiders.PicSpider import PicSpider


__author__ = 'zephyre'


def setup_spider():
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelcrawlerPipeline': 800})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.QiniuyunPipeline': 800})
    crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelNotesPipeline': 800})
    crawler.configure()

    # spider = WeatherSpider()
    # spider = PicSpider()
    spider = TravelNote()
    crawler.crawl(spider)
    crawler.start()

    return spider


def main():
    setup_spider()
    reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    main()