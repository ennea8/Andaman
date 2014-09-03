# coding=utf-8
import sys

import scrapy
<<<<<<< HEAD

#from spiders.QunarPoiSpider import QunarPoiSpider, QunarImageSpider
from spiders.ChanyoujiUserSpiser import ChanyoujiUserSpider
from spiders.ChanyoujiYoujiSpider import ChanyoujiYoujiSpider
from spiders.MafengwoSpider import MafengwoYoujiSpider
sys.path.append('.')

=======
>>>>>>> origin/dev
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor

# from spiders.weather_spider import WeatherSpider
from spiders.notes.baidu_notes import BaiduNoteSpider

import json

__author__ = 'zephyre'


def setup_spider(start, count):
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    settings = crawler.settings

<<<<<<< HEAD
    settings.setdict({'ITEM_PIPELINES': {'pipelines.ChanyoujiUserPipeline': 300,'pipelines.ChanyoujiYoujiPipline':200,'pipelines.MafengwoYoujiPipline':100}})

    settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.ProxySwitchMiddleware': 300})
=======
    settings.setdict({'ITEM_PIPELINES': {'spiders.notes.baidu_notes.BaiduNotePipeline': 100}})

    # settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.TestMiddleware2': 300,
    # 'middlewares.TestMiddleware': 400})
>>>>>>> origin/dev

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.BreadtripPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 400})

    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.ChanyoujiUserPipeline': 300})
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
<<<<<<< HEAD
    spider = MafengwoYoujiSpider()
=======
    # spider = QunarImageSpider()
    spider = BaiduNoteSpider()
>>>>>>> origin/dev

    crawler.crawl(spider)
    crawler.start()
    return spider


def main():
    if len(sys.argv) == 3:
        start = int(sys.argv[1])
        count = int(sys.argv[2])
    else:
        start, count = 0, 0
    setup_spider(start, count)
    scrapy.log.start(loglevel=scrapy.log.INFO)
    reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
<<<<<<< HEAD
    # with open('./scripts/proxy.json', 'r') as f:
    #     proxy = json.load(f)
    #
    # with open('./data/proxy_list.txt', 'w') as f:
    #     for p in proxy:
    #         f.write('%s\n' % p.strip())
    # argv = sys.argv[1]
    # main(argv)
    main()
    Selector
=======
    main()
>>>>>>> origin/dev
