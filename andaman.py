# coding=utf-8
from Queue import Queue
import os
import re
import sys
import imp
from time import time
import traceback
import datetime

import scrapy
from scrapy import signals, Request, Item, log
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


def proc_crawler_settings(crawler):
    """
    读取配置文件，进行相应的设置
    """
    settings = crawler.settings

    config = conf.load_yaml()
    if 'scrapy' in config:
        for key, value in config['scrapy'].items():
            settings.set(key, value)


def setup_spider(spider_name, args):
    import conf

    crawler = Crawler(Settings())
    crawler.signals.connect(reactor.stop, signal=signals.spider_closed)

    proc_crawler_settings(crawler)

    settings = crawler.settings
    ret = parse_args(sys.argv)
    settings.set('USER_PARAM', ret['param'])
    settings.set('USER_ARGS', args)

    settings.set('USER_AGENT', 'Aizou Chrome')

    if args.proxy:
        settings.set('DOWNLOADER_MIDDLEWARES', {'middlewares.ProxySwitchMiddleware': 300})
        settings.set('PROXY_SWITCH_VERIFIER', 'baidu')
        settings.set('PROXY_SWITCH_REFRESH_INTERVAL', 3600)
    settings.set('SPIDER_MIDDLEWARES', {'middlewares.GoogleGeocodeMiddleware': 300})

    settings.set('AUTOTHROTTLE_DEBUG', args.debug)
    settings.set('AUTOTHROTTLE_ENABLED', not args.fast)

    if spider_name in conf.global_conf['spiders']:
        spider_class = conf.global_conf['spiders'][spider_name]
        spider = spider_class.from_crawler(crawler)
        spider_uuid = spider.uuid

        # DRY_RUN: 只抓取，不调用Pipeline
        if not args.dry:
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


item_cnt = 0
item_checkout_cnt = 0
ts_checkpoint = None


def request_proc(req, spider):
    global item_cnt, ts_checkpoint, item_checkout_cnt
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
        item_cnt += 1
        pipeline_proc(spider.pipeline_list, req, spider)
        ts_now = time()
        if ts_now - ts_checkpoint >= 60:
            rate = int((item_cnt - item_checkout_cnt) / (ts_now - ts_checkpoint) * 60)
            ts_checkpoint = ts_now
            item_checkout_cnt = item_cnt

            spider.log('Scraped %d items (at %d items/min)' % (item_cnt, rate), log.INFO)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('crawler')
    parser.add_argument('--fast', action='store_true')
    parser.add_argument('--dry', action='store_true')
    parser.add_argument('--log2file', action='store_true')
    parser.add_argument('--logpath', type=str)
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--proxy', action='store_true')

    args, leftovers = parser.parse_known_args()

    msg = 'SPIDER STARTED: %s' % ' '.join(sys.argv)

    spider_name = args.crawler
    log_path = args.logpath if args.logpath else '/var/log/andaman'
    if args.log2file:
        try:
            os.mkdir(log_path)
        except OSError:
            pass
        logfile = os.path.join(log_path, '%s_%s.log' % (spider_name, datetime.datetime.now().strftime('%Y%m%d')))
    else:
        logfile = None

    s = setup_spider(spider_name, args)
    if s:
        scrapy.log.start(logfile=logfile, loglevel=scrapy.log.DEBUG if args.debug else scrapy.log.INFO)
        s.log(msg, scrapy.log.INFO)

        if 'no-scrapy' in args:
            ts_checkpoint = time()
            s.pipeline_list = s.crawler.engine.scraper.itemproc.middlewares

            for ret in s.start_requests():
                try:
                    request_proc(ret, s)
                except Exception:
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    s.log('Error while processing: %s' % ret)
                    s.log(''.join(lines), log.ERROR)
        else:
            reactor.run()  # the script will block here until the spider_closed signal was sent
    else:
        scrapy.log.start(logfile=logfile, loglevel=scrapy.log.DEBUG if args.debug else scrapy.log.INFO)
        scrapy.log.msg('Cannot find spider: %s' % spider_name, scrapy.log.CRITICAL)


if __name__ == "__main__":
    old_dir = os.getcwd()
    os.chdir(os.path.normpath(os.path.split(__file__)[0]))

    reg_spiders()
    main()

    os.chdir(old_dir)
