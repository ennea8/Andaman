# coding=utf-8

from scrapy.contrib.spiders import CrawlSpider
from items import ZailushangItem
from bs4 import BeautifulSoup
from scrapy import Request
import json
import random
from twisted.internet import defer
import re
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class BreadtripSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(BreadtripSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://breadtrip.com/trips/2387906958/', callback=self.parse_blog)

    def parse_blog(self, response):
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        trip_info = soup.find('div', {'id': 'trip-info'})
        author_url = 'http://breadtrip.com' + trip_info.find('a', {'class': 'trip-user fl'}).get('href')
        print author_url
        title = trip_info.find('div', {'class': 'trip-summary fl'}).find('h2').getText()
        print title
        dd = trip_info.find('p').findAll('span')
        rr = []
        for it in dd:
            # print it
            rr.append(it.getText())
        date = rr[0]
        print date
        days = rr[1]
        print days
        ss = trip_info.find('div', {'class': 'trip-tools ibfix fr'}).findAll('b')
        ll = []
        for it in ss:
            ll.append(it.getText())
        like_num = ll[0]
        cmt_num = ll[1]
        share_num = ll[2]
        print like_num
        print cmt_num
        print share_num
        right = soup.find('div', {'class': 'panel-content'})
        sis = right.findAll('p', {'class': 'poi-info'})
        sights = []
        for it in sis:
            print it.getText()
            sights.append(it.getText())



