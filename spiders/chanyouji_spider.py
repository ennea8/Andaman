

__author__ = 'wdx'
# coding=utf-8

import pymongo
import copy
import re
from scrapy import Request, Selector
import scrapy
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiItem


class ChanyoujiUserSpider(CrawlSpider):


    name = 'chanyouji'

    allowed_domains = ["chanyouji.com"]

    def start_requests(self):
        user_id = 1;
        url_template = 'http://chanyouji.com/users/%d'

        for user_id in range(100):
            url = url_template % user_id
            m={'user_id':user_id}
            yield Request(url=url, callback=self.parse, meta={'data':m})


    def parse(self,response):
        items=[]

        sel=Selector(response)
        item = ChanyoujiItem()
        user_data=response.meta['data']
        item['user_id']=user_data['user_id']
        item['user_name']=sel.xpath('//div[contains(@class, "header-inner")]/h1/text()').extract()
        item['num_youji']=sel.xpath('//div[contains(@class, "header-inner")]/div[1]/text()').extract()
        #item['num_youji']=re.search('[0-9]*',num_youji[0]).group()
        item['weibo_url']=sel.xpath('//div[contains(@class, "header-inner")]//li[contains(@class,"weibo")]/a/@href').extract()
        items.append(item)
        return items




        






