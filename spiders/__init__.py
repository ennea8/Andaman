# coding=utf-8
# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import hashlib
import random
import time
import sys

from scrapy import log

from scrapy.contrib.spiders import CrawlSpider


class AizouCrawlSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        super(CrawlSpider, self).__init__(*a, **kw)

        # 每个爬虫需要分配一个唯一的爬虫id，用来在日志文件里面作出区分。
        r = long(time.time() * 1000) + random.randint(0, sys.maxint)
        h = hashlib.md5('%d' % r).hexdigest()
        self.spider_id = h[:16]

    def log(self, message, level=log.DEBUG, **kw):
        # message前面添加爬虫id
        message = '[%s] %s' % (self.spider_id, message)
        super(CrawlSpider, self).log(message, level=level, **kw)