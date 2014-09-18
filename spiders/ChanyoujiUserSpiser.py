import pymongo

__author__ = 'wdx'
# coding=utf-8

import re

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiUser


class ChanyoujiUserSpider(CrawlSpider):
    name = 'chanyouji_user'

    allowed_domains = ["chanyouji.com"]

    def start_requests(self):
        url_template = 'http://chanyouji.com/users/%d'

        # for user_id in range(1, 400000):
        for user_id in range(1, 11):
            url = url_template % user_id
            m = {'user_id': user_id}
            yield Request(url=url, callback=self.parse, meta={'data': m})

    def parse(self, response):
        sel = Selector(response)
        item = ChanyoujiUser()
        user_data = response.meta['data']
        item['user_id'] = user_data['user_id']
        user_name = sel.xpath('//div[contains(@class, "header-inner")]/h1/text()').extract()
        if user_name:
            item['user_name'] = user_name[0]
        else:
            item['user_name'] = None
        num_youji = sel.xpath('//div[contains(@class, "header-inner")]/div[1]/text()').extract()[0]
        num = re.compile('\d{1,}')
        m1 = num.search(num_youji)
        if m1:
            item['num_notes'] = int(m1.group())
        # item['num_youji']=re.search('[0-9]*',num_youji[0]).group()

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"weibo")]/a/@href').extract()
        if ret:
            weibo_url = ret[0]
            item['weibo_url'] = weibo_url

            match = re.search(r'weibo\.com/u/(\d+)/?$', weibo_url)
            if match:
                item['weibo_uid'] = int(match.groups()[0])
            else:
                match = re.search(r'weibo\.com/([^/]+)/?$', weibo_url)
                if match:
                    item['weibo_uid'] = match.groups()[0]

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"douban")]/a/@href').extract()
        if ret:
            douban_url = ret[0]
            item['douban_url'] = douban_url
            match = re.search(r'douban\.com/people/(\d+)/?$', douban_url)
            if match:
                item['douban_uid'] = int(match.groups()[0])

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"renren")]/a/@href').extract()
        if ret:
            renren_url = ret[0]
            item['renren_url'] = renren_url
            match = re.search(r'renren\.com/(\d+)/profile/?$', renren_url)
            if match:
                item['renren_uid'] = int(match.groups()[0])

        traveled = sel.xpath('//ul[@id="attraction_markers_list"]/li/a/span/text()').extract()
        if traveled:
            item['traveled'] = traveled

        yield item


class ChanyoujiUserPipeline(object):
    def __init__(self):
        self.db = None

    def connect(self):
        self.db = pymongo.MongoClient().Chanyoujidb

    def process_item(self, item, spider):
        if not isinstance(item, ChanyoujiUser):
            return item

        col = pymongo.MongoClient().raw_data.ChanyoujiUser

        uid = item['user_id']
        data = {'userId': uid,
                'userName': item['user_name'],
                'numNotes': item['num_notes']}

        if 'weibo_url' in item:
            data['weiboUrl']=item['weibo_url']
        if 'weibo_uid' in item:
            data['weiboUid']=item['weibo_uid']
        if 'renren_url' in item:
            data['renrenUrl']=item['renren_url']
        if 'renren_uid' in item:
            data['renrenUid']=item['renren_uid']
        if 'douban_url' in item:
            data['doubanUrl']=item['douban_url']
        if 'douban_uid' in item:
            data['doubanUid']=item['douban_uid']
        if 'traveled' in item:
            data['traveled'] = item['traveled']

        ret = col.find_one({'userId': uid})
        if ret:
            data['_id'] = ret['_id']
        col.save(data)

        return item









