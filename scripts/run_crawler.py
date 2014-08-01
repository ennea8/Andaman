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

__author__ = 'zephyre'


'''def setup_spider(argv):
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelcrawlerPipeline': 800})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 800})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 800})
    crawler.configure()
    #if argv=='Travel':
    #    spider = WeatherSpider()
    if argv == 'mafengwo':
        spider = MafengwoSpider()
    elif argv == 'yiqiqu':
        spider = YiqiquSpider()
    elif argv == 'zailushang':
        spider = ZailushangSpider()
    elif argv == 'breaktrip':
        spider = BreadtripSpider()
    crawler.crawl(spider)
    crawler.start()

    return spider


def main(argv):
    setup_spider(argv)
    reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    argv = sys.argv[1]
    main(argv)'''
def setup_spider():
    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.TravelcrawlerPipeline': 800})
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 800})
    crawler.settings.set('ITEM_PIPELINES', {'pipelines.MyImagesPipeline': 900})
    crawler.settings.set('IMAGES_STORE', './zailushang')
    crawler.settings.set('IMAGES_MIN_HEIGHT', 110)
    crawler.settings.set('IMAGES_MIN_WIDTH', 110)
    #crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 800})
    crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 800})
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
    spider=ZailushangSpider()
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