# coding=utf-8
import ConfigParser
import os

import MySQLdb
from MySQLdb.cursors import DictCursor
from scrapy.contrib.spiders import CrawlSpider

import utils


__author__ = 'zephyre'


class MafengwoTargetSpider(CrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(MafengwoTargetSpider, self).__init__(*a, **kw)

    @staticmethod
    def get_config(section, key):
        config = ConfigParser.ConfigParser()
        d = os.path.split(os.path.realpath(__file__))[0]
        path = os.path.realpath(os.path.join(d, '../private.cfg'))
        config.read(path)
        return config.get(section, key)

    def start_requests(self):
        host = self.get_config('cms-mysqldb', 'host')
        port = self.get_config('cms-mysqldb', 'port')
        user = self.get_config('cms-mysqldb', 'user')
        passwd = self.get_config('cms-mysqldb', 'passwd')
        my_conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db='lvplan', cursorclass=DictCursor,
                                  charset='utf8')
        cursor = my_conn.cursor()
        cursor.execute('SELECT * FROM lvplan.mfw_target_url')

        col = utils.get_mongodb('')

        for row in cursor:
            pass

        pass
