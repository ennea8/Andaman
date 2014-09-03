# -*- coding: UTF-8 -*-
__author__ = 'wdx'

from scrapy import Request,Selector
from scrapy.contrib.spiders import CrawlSpider
from items import MafengwoYoujiItem


class MafengwoYoujiSpider(CrawlSpider):

    name = "mafengwo_youji"

    def start_requests(self):
        id=[10065,10189,10099,10819,10320,10198,10208,10206,10030,10095,10186,10487,10482,10807,10121,10651,10809,15950,10650,10156,10010,10434,10155,10445,11534,10129,11505,10124,10035,10136,10061,10143,10564,14727,10510,11327,10134,10011,10207,10684,10128,10140,11729,10435,10804,15967,14458,10442,10814,10417,11474,10073,10079,11606,10811,10732,10757,10218,10758,10469,11471,10468,24090,10088,10269,10507,10779,10778,13063,18381,11475,10796,10453,10301,10381,21434,10219,10799,10390,10045,10244]
        template_url="http://www.mafengwo.cn/yj/%d/1-0-%d.html"
        for yj_id in id:
            m = {'template_url':template_url,'yj_id':yj_id,'page':1}
            url=template_url % (m['yj_id'],m['page'])
            yield Request(url=url,callback=self.parse,meta={'data':m})



    def parse(self, response):

        sel=Selector(response)
        data=response.meta['data']
        page=data['page']
        yj_id=data['yj_id']
        template_url=data['template_url']
        max_youji_page_m=sel.xpath('//div[@class="page-hotel"]/a[@class="ti"]/text()').extract()
        if max_youji_page_m:
            max_youji_page=max_youji_page_m[-1]

        place=sel.xpath('//div[@class="mdd-title"]/h1/text()').extract()
        if place:
            m={'place':place[0]}
        else:
            return

        post_list=sel.xpath('//div[@class="post-list"]/ul/li[contains(@class,"post-item")]')
        for post_item in post_list:
            url_m=post_item.xpath('./h2/a/@href').extract()
            if url_m:
                url=url_m[0]
            else:
                return
            youji_url="http://www.mafengwo.cn%s" % url
            title=post_item.xpath('./h2/a/text()').extract()
            if title:
                m['title']=title[0]
            else:
                return
            yield Request(url=youji_url,callback=self.parse_youji,meta={'data':m})

        if page!=max_youji_page:

            page=page+1
            page_url=template_url % (yj_id,page)
            page_mm={'page':page,'yj_id':yj_id,'template_url':template_url}
            yield Request(url=page_url,callback=self.parse,meta={'data':page_mm})

    def parse_youji(self,response):

        sel=Selector(response)
        #item=MafengwoYoujiItem()
        m={}
        item={}
        author=sel.xpath('//div[contains(@class,"author_info")]//img/@title').extract()
        meta = response.meta['data']
        item['place']=meta['place']
        item['title']=meta['title']
        if author:
            item['author']=author[0]
        else:
            item['author']=None
        public_time=sel.xpath('//div[@class="basic-info"]//li[@class="item-date"]//b/text()').extract()
        if public_time:
            item['public_time']=public_time[0]
        else:
            item['public_time']=None
        cost=sel.xpath('//div[@class="basic-info"]//li[@class="item-cost"]//b/text()').extract()
        if cost:
            item['cost']=cost[0]
        else:
            item['cost']=None
        way=sel.xpath('//div[@class="basic-info"]//li[@class="item-type"]//b/text()').extract()
        if way:
            item['way']=way[0]
        else:
            item['way']=None
        days=sel.xpath('//div[@class="basic-info"]/ul/li[@class="item-days"]//b/text()').extract()
        if days:
            item['days']=days[0]
        else:
            item['days']=None
        contents=sel.xpath('//div[@class="post_item"]//div[@class="a_con_text cont"]').extract()
        if contents:
            item['contents']=contents
        else:
            item['contents']=None
        reply=sel.xpath('//div[@class="post_item"]/div/div[contains(@class,"a_con_text reply")]').extract()
        if reply:
            item['reply']=reply
        else:
            item['reply']=None
        url_page=sel.xpath('//a[@class="ti"]/@href').extract()
        if url_page:
            max_page=sel.xpath('//a[@class="ti"]/@data-value').extract()[-1]
            url_template=url_page[0][:-1]
            # for page in range(2,max_page):
            url="http://www.mafengwo.cn%s%d" % (url_template,2)
            yield Request(url=url,callback=self.parse_reply,meta={'item':item, 'page': 2,'max_page':max_page,'url':url})

    def parse_reply(self,response):

        sel=Selector(response)
        item=MafengwoYoujiItem()
        item=response.meta['item']
        page = response.meta['page']
        max_page = response.meta['max_page']
        template_url = response.meta['url'][:-1]

        reply=sel.xpath('//div[@class="post_item"]/div/div[contains(@class,"a_con_text reply")]').extract()
        if reply:
            item['reply'].extend(reply)

        if page!=max_page:
            page+=1
            url = "%s%d" % (template_url,page)
            yield Request(url=url, callback=self.parse_reply, meta={'item': item, 'page': page ,'max_page':max_page ,'url':url})
        else:
            yield item



















