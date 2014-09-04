# coding=utf-8
import sys

import scrapy
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor

# from spiders.weather_spider import WeatherSpider

__author__ = 'zephyre'


def setup_spider(spider_name):
    import conf

    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    settings = crawler.settings

    settings.setdict({'ITEM_PIPELINES': {tmp: 100 for tmp in conf.global_conf[
        'pipelines']}})  # {'spiders.notes.baidu_notes.BaiduNotePipeline': 100}})

    # settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.TestMiddleware2': 300,
    # 'middlewares.TestMiddleware': 400})

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.BreadtripPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 400})

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ChanyoujiUserPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 100})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 200})

    # crawler.settings.set('ITEM_PIPELINES', {'scrapy.contrib.pipeline.images.ImagesPipeline': 500})

    settings.set('IMAGES_MIN_HEIGHT', 110)
    settings.set('IMAGES_MIN_WIDTH', 110)

    crawler.configure()

    if spider_name in conf.global_conf['spiders']:
        spider = conf.global_conf['spiders'][spider_name]()

        crawler.crawl(spider)
        crawler.start()
        return spider
    else:
        return None


def reg_spiders(spider_dir=None):
    """
    将spiders路径下的爬虫类进行注册
    """
    import os
    import imp
    from scrapy.contrib.spiders import CrawlSpider
    import conf

    if not spider_dir:
        root_dir = os.path.normpath(os.path.join(os.path.split(__file__)[0], '..'))
        spider_dir = os.path.normpath(os.path.join(root_dir, 'spiders'))

    conf.global_conf['spiders'] = {}
    conf.global_conf['pipelines'] = []

    for cur, d_list, f_list in os.walk(spider_dir):

        # 获得包路径
        package_path = []
        tmp = cur
        while True:
            d1, d2 = os.path.split(tmp)
            package_path.insert(0, d2)
            if d2 == 'spiders' or d1 == '/' or not d1:
                break
            tmp = d1
        package_path = '.'.join(package_path)

        for f in f_list:
            f = os.path.normpath(os.path.join(cur, f))
            tmp, ext = os.path.splitext(f)
            if ext != '.py':
                continue
            p, fname = os.path.split(tmp)

            try:
                ret = imp.find_module(fname, [p]) if p else imp.find_module(fname)
                mod = imp.load_module(fname, *ret)

                for attr_name in dir(mod):
                    try:
                        c = getattr(mod, attr_name)
                        if issubclass(c, CrawlSpider) and c != CrawlSpider:
                            name = getattr(c, 'name')
                            if name:
                                conf.global_conf['spiders'][name] = c
                        elif hasattr(c, 'process_item'):
                            conf.global_conf['pipelines'].append(package_path + '.' + c.__module__ + '.' + c.__name__)

                    except TypeError:
                        pass
            except ImportError:
                pass


def main():
    spider_name = sys.argv[1]
    # if len(sys.argv) == 3:
    # start = int(sys.argv[1])
    # count = int(sys.argv[2])
    # else:
    # start, count = 0, 0
    s = setup_spider(spider_name)
    if s:
        scrapy.log.start(loglevel=scrapy.log.INFO)
        reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    reg_spiders()
    main()