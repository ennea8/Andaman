# coding=utf-8
# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
import hashlib
import random
import time
import sys
import urlparse

from scrapy.contrib.spiders import CrawlSpider

import utils


class AizouCrawlSpider(CrawlSpider):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings.get("USER_PARAM"))

    def __init__(self, param, *a, **kw):
        super(CrawlSpider, self).__init__(*a, **kw)
        self.param = param

        # 每个爬虫需要分配一个唯一的爬虫id，用来在日志文件里面作出区分。
        r = long(time.time() * 1000) + random.randint(0, sys.maxint)
        h = hashlib.md5('%d' % r).hexdigest()
        self.name = '%s:%s' % (self.name, h[:16])

    def build_href(self, url, href):
        c = urlparse.urlparse(href)
        if c.netloc:
            return href
        else:
            c1 = urlparse.urlparse(url)
            return urlparse.urlunparse((c1.scheme, c1.netloc, c.path, c.params, c.query, c.fragment))


class AizouPipeline(object):
    spiders = []
    spiders_uuid = []

    def fetch_db_col(self, db, col, profile):
        sig = '%s.%s.%s' % (db, col, profile)
        if sig not in self.col_dict:
            self.col_dict[sig] = utils.get_mongodb(db, col, profile)
        return self.col_dict[sig]

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings.get("USER_PARAM"))

    def __init__(self, param):
        self.param = param
        self.col_dict = {}

    def is_handler(self, item, spider):
        return spider.uuid in self.spiders_uuid