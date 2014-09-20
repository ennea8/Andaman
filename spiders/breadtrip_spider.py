# coding=utf-8

import json
import random
import sys

from scrapy.contrib.spiders import CrawlSpider
from bs4 import BeautifulSoup
from scrapy import Request

from items import BreadtripItem


reload(sys)
sys.setdefaultencoding('utf8')


class BreadtripSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'weather'
        super(BreadtripSpider, self).__init__(*a, **kw)

    def start_requests(self):
        with open('proxy.json', 'r')as file:
            t = file.readline()
            proxy_list = json.loads(t)
            i = random.randint(0, len(proxy_list) - 1)
        Max_Request_Nums = 5
        for id in range(2387000000, 2387999999):
            yield Request(url="http://breadtrip.com/trips/%s/schedule_line/" % id, callback=self.parse_sights,
                          meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))


    def parse_sights(self, response):
        proxy_list = response.meta['proxy_list']
        soup = BeautifulSoup(response.body, from_encoding='utf8')
        if not soup.find('div', {'class': 'error-page err-404'}):
            pois = soup.findAll('dd', {'class': 'poi-panel'})
            for poi in pois:
                ci = poi.find('div', {'class': 'city-info'})
                sights = ci.find('span', {'class': 'one-row-ellipsis'}).getText()
                if poi.findAll('ul', {'class': 'poi'}):
                    sis = poi.findAll('ul', {'class': 'poi'})
                    for si in sis:
                        fns = si.findAll('li', {'class': 'fn-clear'})
                        for fn in fns:
                            st = fn.find('a', {'class': 'poi-name one-row-ellipsis'}).getText()
                            sights = sights + ',' + st
            i = random.randint(0, len(proxy_list) - 1)
            Max_Request_Nums = 5
            for id in range(2387000000, 2387999999):
                url = 'http://breadtrip.com/trips/%s/' % id
                yield Request(url='http://breadtrip.com/trips/%s/' % id, callback=self.parse_blog,
                              meta={'proxy': "http://%s" % proxy_list[i], 'url': url, 'sights': sights,
                                    'proxy_list': proxy_list,
                                    'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))
        else:
            print '======wrong page'

    def parse_blog(self, response):
        item = BreadtripItem()
        sights = response.meta['sights']
        print sights
        blog_url = response.meta['url']
        print blog_url
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
        content = soup.find('div', {'class': 'trip-wps'})
        conts = str(content)
        # print content
        dds = content.findAll('div', {'class': 'trip-days'})
        image_urls = []
        for dd in dds:
            image_urls.append(dd.find('img', {'class': 'photo'}).get('data-original'))
        print image_urls
        item['web_name'] = 'breadtrip'
        item['author_url'] = author_url
        item['blog_url'] = blog_url
        item['title'] = title
        item['date'] = date
        item['days'] = days
        # item['tag']=tag
        item['like_num'] = like_num
        item['image_urls'] = image_urls
        item['cmt_num'] = cmt_num
        item['share_num'] = share_num
        item['sights'] = sights
        item['content'] = conts
        yield item

    def SetProxy(self, Max_Request_Nums):
        Max_Request_Nums = Request.meta['Max_Request_nums']
        Max_Request_Nums = Max_Request_Nums - 1
        if Max_Request_Nums > 0:
            with open('proxy.json', 'r')as file:
                t = file.readline()
                proxy_list = json.loads(t)
                i = random.randint(0, len(proxy_list) - 1)
                for id in range(2387000000, 2387999999):
                    yield Request(url="http://breadtrip.com/trips/%s/schedule_line/" % id, callback=self.parse_sights,
                                  meta={'proxy': "http://%s" % proxy_list[i], 'proxy_list': proxy_list,
                                        'Max_Request_nums': Max_Request_Nums}, errback=self.SetProxy(Max_Request_Nums))

        else:
            return



