

__author__ = 'wdx'
# coding=utf-8

import pymongo
import copy
import re
from scrapy import Request, Selector
import scrapy
from scrapy.contrib.spiders import CrawlSpider
from pymongo import MongoClient
import time
import base64

from items import ChanyoujiItem



class ChanyoujiUserSpider(CrawlSpider):


    name = 'chanyouji'

    allowed_domains = ["chanyouji.com"]

    def start_requests(self):

        user_id = 1;
        url_template = 'http://chanyouji.com/users/%d'

        for user_id in range(1,400000):
            url = url_template % user_id
            m={'user_id':user_id}
            #time.sleep(0.5)
            yield Request(url=url, callback=self.parse, meta={'data':m})


    def parse(self,response):
        items=[]

        sel=Selector(response)
        item = ChanyoujiItem()
        user_data=response.meta['data']
        item['user_id']=user_data['user_id']
        user_name=sel.xpath('//div[contains(@class, "header-inner")]/h1/text()').extract()
        if user_name:
            item['user_name']=user_name[0]
        else:
            item['user_name']=None
        num_youji=sel.xpath('//div[contains(@class, "header-inner")]/div[1]/text()').extract()[0]
        num=re.compile('\d{1,}')
        m1=num.search(num_youji)
        if m1:
            item['num_youji']=m1.group()
            print
        else:
            item['num_youji']=None
        #item['num_youji']=re.search('[0-9]*',num_youji[0]).group()
        weibo_url=sel.xpath('//div[contains(@class, "header-inner")]//li[contains(@class,"weibo")]/a/@href').extract()
        if weibo_url:
            item['weibo_url']=weibo_url[0]
        else:
            item['weibo_url']=None

        triped=sel.xpath('//ul[@class="clearfix"]//span/text()').extract()
        if triped:
            item['triped']=triped
        else:
            item['triped']=None
        items.append(item)
        return items











