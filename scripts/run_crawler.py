# coding=utf-8
import sys

import scrapy
#from spiders.MafengwoSpider import MafengwoYoujiSpider

from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor

# from spiders.weather_spider import WeatherSpider
#from spiders.notes.baidu_notes import BaiduNoteSpider
#from spiders.MafengwoSpider import MafengwoYoujiSpider
from spiders.ChanyoujiYoujiSpider import ChanyoujiYoujiSpider

__author__ = 'zephyre'


def setup_spider():
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    settings = crawler.settings

    # settings.setdict({'ITEM_PIPELINES': {'spiders.notes.baidu_notes.BaiduNotePipeline': 200,'pipelines.ChanyoujiYoujiPipline':100}})
    # settings.setdict({'ITEM_PIPELINES': {'pipelines.ChanyoujiYoujiPipline':800}})
    settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.ProxySwitchMiddleware': 300})

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.BreadtripPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 400})

    crawler.settings.set('ITEM_PIPELINES', {'pipelines.ChanyoujiYoujiPipline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 100})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 200})

    # crawler.settings.set('ITEM_PIPELINES', {'scrapy.contrib.pipeline.images.ImagesPipeline': 500})

    settings.set('IMAGES_MIN_HEIGHT', 110)
    settings.set('IMAGES_MIN_WIDTH', 110)

    crawler.configure()
    # if argv=='Travel':
    # spider = WeatherSpider()
    '''if argv == 'mafengwo':
        spider = MafengwoSpider()
    elif argv == 'yiqiqu':
        spider = YiqiquSpider()
    elif argv == 'zailushang':
        spider = ZailushangSpider()
    elif argv == 'breaktrip':
        spider = BreadtripSpider()'''
    # spider=MafengwoSpider()
    # spider=ZailushangSpider()
    # spider=BreadtripSpider()
    # spider = YiqiquSpider()
    # spider = BaiduPoiSpider()

    # spider = QunarPoiSpider(2)
    # spider = QunarImageSpider()
    spider = ChanyoujiYoujiSpider()

    crawler.crawl(spider)
    crawler.start()
    return spider


def main():
    #if len(sys.argv) == 3:
    #    start = int(sys.argv[1])
    #    count = int(sys.argv[2])
    #else:
    #    start, count = 0, 0
    setup_spider()
    scrapy.log.start(loglevel=scrapy.log.INFO)
    reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    main()