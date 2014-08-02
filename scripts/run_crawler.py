# coding=utf-8
import sys
sys.path.append('.')
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor
# from spiders.weather_spider import WeatherSpider
from spiders.mofengwo_spider import MafengwoSpider
from spiders.zailushang_spider import ZailushangSpider
from spiders.breadtrip_spider import  BreadtripSpider
from spiders.yiqiqu_spider import YiqiquSpider

__author__ = 'zephyre'


def setup_spider():
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelcrawlerPipeline': 800})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 100})
    crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 200})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.BreadtripPipeline': 300})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 400})
    crawler.settings.set('ITEM_PIPELINES', {'scrapy.contrib.pipeline.images.ImagesPipeline': 500})
    crawler.settings.set('IMAGES_STORE', 'F:\images\yiqiqu')
    '''if argv == 'mafengwo':
         crawler.settings.set('IMAGES_STORE', 'F:\images\mafengwo')
    elif argv == 'yiqiqu':
         crawler.settings.set('IMAGES_STORE', 'F:\images\yiqiqu')
    elif argv == 'zailushang':
         crawler.settings.set('IMAGES_STORE', 'F:\images\zailushang')
    elif argv == 'breaktrip':
         crawler.settings.set('IMAGES_STORE', 'F:\images\breaktrip')'''
    crawler.settings.set('IMAGES_MIN_HEIGHT', 110)
    crawler.settings.set('IMAGES_MIN_WIDTH', 110)
    crawler.configure()
    #if argv=='Travel':
    #    spider = WeatherSpider()
    '''if argv == 'mafengwo':
        spider = MafengwoSpider()
    elif argv == 'yiqiqu':
        spider = YiqiquSpider()
    elif argv == 'zailushang':
        spider = ZailushangSpider()
    elif argv == 'breaktrip':
        spider = BreadtripSpider()'''
    #spider=MafengwoSpider()
    #spider=ZailushangSpider()
    #spider=BreadtripSpider()
    spider=YiqiquSpider()
    crawler.crawl(spider)
    crawler.start()
    return spider
def main():
    setup_spider()
    reactor.run()  # the script will block here until the spider_closed signal was sent

if __name__ == "__main__":
    #argv = sys.argv[1]
    #main(argv)
    main()