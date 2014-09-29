# coding=utf-8
from Queue import Queue
import re
import sys

import scrapy
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from twisted.internet import reactor






# from spiders.weather_spider import WeatherSpider

__author__ = 'zephyre'


def parse_args(args):
    """
    解析命令行的参数
    @param args:
    @return:
    """
    if len(args) == 1:
        return {'cmd': None, 'param': None}

    cmd = args[1]
    # 如果以-开头，说明不是cmd，而是参数列表
    if re.search('^\-', cmd):
        cmd = None
        param_idx = 1
    else:
        param_idx = 2

    # 解析命令行参数
    param_dict = {}
    q = Queue()
    for tmp in args[param_idx:]:
        q.put(tmp)
    param_name = None
    param_value = None
    while not q.empty():
        term = q.get()
        if re.search(r'^--(?=[^\-])', term):
            tmp = re.sub('^-+', '', term)
            if param_name:
                param_dict[param_name] = param_value
            param_name = tmp
            param_value = None
        elif re.search(r'^-(?=[^\-])', term):
            tmp = re.sub('^-+', '', term)
            for tmp in list(tmp):
                if param_name:
                    param_dict[param_name] = param_value
                    param_value = None
                param_name = tmp
        else:
            if param_name:
                if param_value:
                    param_value.append(term)
                else:
                    param_value = [term]
    if param_name:
        param_dict[param_name] = param_value

    # # debug和debug-port是通用参数，表示将启用远程调试模块。
    # if 'debug' in param_dict:
    # if 'debug-port' in param_dict:
    # port = int(param_dict['debug-port'][0])
    # else:
    # port = getattr(glob, 'DEBUG')['DEBUG_PORT']
    # import pydevd
    #
    #     pydevd.settrace('localhost', port=port, stdoutToServer=True, stderrToServer=True)

    return {'cmd': cmd, 'param': param_dict}


def setup_spider(spider_name, param={}):
    import conf

    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    settings = crawler.settings

    settings.setdict({'ITEM_PIPELINES': {tmp: 100 for tmp in conf.global_conf[
        'pipelines']}})
    settings.set('LOG_LEVEL', 'INFO')

    if 'proxy' in param:
        settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.ProxySwitchMiddleware': 300})

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.BreadtripPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ZailushangPipeline': 400})

    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.ChanyoujiUserPipeline': 300})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.MofengwoPipeline': 100})
    # crawler.settings.set('ITEM_PIPELINES', {'pipelines.YiqiquPipeline': 200})

    # crawler.settings.set('ITEM_PIPELINES', {'scrapy.contrib.pipeline.images.ImagesPipeline': 500})

    # settings.set('IMAGES_MIN_HEIGHT', 110)
    # settings.set('IMAGES_MIN_WIDTH', 110)

    crawler.configure()

    if spider_name in conf.global_conf['spiders']:
        spider = conf.global_conf['spiders'][spider_name]()

        crawler.crawl(spider)
        crawler.start()
        setattr(spider, 'param', param)
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
        root_dir = os.path.normpath(os.path.split(__file__)[0])
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
    ret = parse_args(sys.argv)
    if not ret:
        return

    spider_name = ret['cmd']
    param = ret['param']
    s = setup_spider(spider_name, param)
    if s:
        scrapy.log.start(loglevel=scrapy.log.INFO)
        reactor.run()  # the script will block here until the spider_closed signal was sent


if __name__ == "__main__":
    reg_spiders()
    main()