# coding=utf-8
from Queue import Queue
import os
import re
import sys
import imp

import datetime
import scrapy
from scrapy import signals, Request, Item
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.settings import Settings
from twisted.internet import reactor

import conf
from spiders import AizouPipeline, AizouCrawlSpider


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
    # pydevd.settrace('localhost', port=port, stdoutToServer=True, stderrToServer=True)

    return {'cmd': cmd, 'param': param_dict}


def setup_spider(spider_name, param):
    import conf

    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    settings = crawler.settings
    settings.set('USER_PARAM', param)

    if 'proxy' in param:
        settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.ProxySwitchMiddleware': 300})
        settings.set('PROXY_SWITCH_VERIFIER', param['proxy-verifier'][0] if 'proxy-verifier' in param else 'baidu')
    settings.set('SPIDER_MIDDLEWARES', {'middlewares.GoogleGeocodeMiddleware': 300})

    settings.set('AUTOTHROTTLE_DEBUG', 'debug' in param)
    settings.set('AUTOTHROTTLE_ENABLED', 'fast' not in param)

    if spider_name in conf.global_conf['spiders']:
        spider_class = conf.global_conf['spiders'][spider_name]
        spider = spider_class.from_crawler(crawler)
        spider_uuid = spider.uuid

        # DRY_RUN: 只抓取，不调用Pipeline
        if 'dry' not in param:
            # 查找对应的pipeline
            settings.set('ITEM_PIPELINES', {tmp[0]: 100 for tmp in
                                            filter(lambda p: spider_uuid in p[1], conf.global_conf['pipelines'])})

        crawler.configure()
        crawler.crawl(spider)
        crawler.start()
        # setattr(spider, 'param', param)
        return spider
    else:
        return None


def reg_spiders(spider_dir=None):
    """
    将spiders路径下的爬虫类进行注册
    """
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
                        if issubclass(c, AizouCrawlSpider) and c != AizouCrawlSpider:
                            name = getattr(c, 'name')
                            if name:
                                conf.global_conf['spiders'][name] = c
                        elif issubclass(c, AizouPipeline) and c != AizouPipeline:
                            conf.global_conf['pipelines'].append(
                                [package_path + '.' + c.__module__ + '.' + c.__name__, getattr(c, 'spiders_uuid', [])])
                    except TypeError:
                        pass
            except ImportError:
                print 'Import error: %s' % fname
                raise


def pipeline_proc(pipeline_list, item, spider):
    for p in pipeline_list:
        item = p.process_item(item, spider)
        if not item:
            break


def request_proc(req, spider):
    if isinstance(req, Request):
        callback = req.callback
        if not callback:
            callback = spider.parse
        response = Response('http://www.baidu.com', request=req)
        ret = callback(response)
        if hasattr(ret, '__iter__'):
            for entry in ret:
                request_proc(entry, spider)
        else:
            request_proc(ret, spider)
    elif isinstance(req, Item):
        pipeline_proc(spider.pipeline_list, req, spider)


def main():
    ret = parse_args(sys.argv)
    if not ret:
        return

    msg = 'SPIDER STARTED: %s' % ' '.join(sys.argv)

    spider_name = ret['cmd']
    param = ret['param']
    s = setup_spider(spider_name, param)
    if s:
        if 'verbose' in param:
            logfile = None
        else:
            logfile = './logs/%s_%s.log' % (spider_name, datetime.datetime.now().strftime('%Y%m%d'))
        scrapy.log.start(logfile=logfile, loglevel=scrapy.log.DEBUG if 'debug' in param else scrapy.log.INFO)
        s.log(msg, scrapy.log.INFO)

        if 'no-scrapy' in param:
            s.pipeline_list = s.crawler.engine.scraper.itemproc.middlewares

            for ret in s.start_requests():
                request_proc(ret, s)
        else:
            reactor.run()  # the script will block here until the spider_closed signal was sent
    else:
        if 'verbose' in param:
            logfile = None
        else:
            logfile = './logs/error.log'
        scrapy.log.start(logfile=logfile, loglevel=scrapy.log.DEBUG if 'debug' in param else scrapy.log.INFO)
        scrapy.log.msg('Cannot find spider: %s' % spider_name, scrapy.log.CRITICAL)


if __name__ == "__main__":
    old_dir = os.getcwd()
    os.chdir(os.path.normpath(os.path.split(__file__)[0]))

    reg_spiders()
    main()

    os.chdir(old_dir)