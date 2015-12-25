# coding=utf-8
from urlparse import urljoin
import re
import logging

import scrapy
from scrapy.http import Request
from scrapy.http import FormRequest

from andaman.items.jieban import JiebanItem


class CtripJiebanSpider(scrapy.Spider):
    """
    抓取Ctrip的结伴信息
    """
    name = 'ctrip-jieban'

    def start_requests(self):
        start_urls = [
            'http://vacations.ctrip.com/tours',
            'http://vacations.ctrip.com/tours/inter'
        ]
        for url in start_urls:
            yield Request(url)

    def parse(self, response):

        # 爬取城市列表
        for city in response.xpath('//div[@class="sel_list"]/dl/dd/a/@href').extract():
            num = int(re.search(r'\d+', str(city)).group(0))
            url = 'http://you.ctrip.com/DangdiSite/events/%d.html' % num
            yield Request(url, callback=self.parse_city)

    def parse_city(self, response):

        # 爬取每个城市对应的页面的文章列表
        for href in response.xpath('//ul[@class="cf"]/li/a/@href').extract():
            url = urljoin(response.url, href)
            yield Request(url, callback=self.parse_article)

    def parse_article(self, response):
        item = JiebanItem()
        item['source'] = 'ctrip'
        item['title'] = response.xpath('//title/text()').extract()[0]
        item['tid'] = int(response.url.split('/')[5].split('.')[0])
        if response.xpath(
                '//div[@class="gsn-inputbox"]/input[@id="receiver_id"]/../input[@type="text"]/@value').extract():
            item['author'] = response.xpath(
                '//div[@class="gsn-inputbox"]/input[@id="receiver_id"]/../input[@type="text"]/@value').extract()[0]
        else:
            item['author'] = ''
        eventsummaryinfoview = response.xpath('//div[@id="eventsummaryinfoview"]')
        if eventsummaryinfoview.xpath('./p/span[@class="littlepadding"]/text()').extract():
            item['start_time'] = eventsummaryinfoview.xpath('./p/span[@class="littlepadding"]/text()').extract()[0]
        else:
            item['start_time'] = ''
        if eventsummaryinfoview.xpath('//p[@class="events_time"]/text()').extract():
            item['days'] = eventsummaryinfoview.xpath('//p[@class="events_time"]/text()').extract()[2]
        else:
            item['days'] = ''
        if eventsummaryinfoview.xpath('//p[@class="events_place"]/text()').extract():
            item['departure'] = eventsummaryinfoview.xpath('//p[@class="events_place"]/text()').extract()[1]
        else:
            item['departure'] = ''
        if eventsummaryinfoview.xpath('//p[@class="events_place"]/text()').extract():
            item['destination'] = eventsummaryinfoview.xpath('//p[@class="events_place"]/text()').extract()[2]
        else:
            item['destination'] = ''
        if eventsummaryinfoview.xpath('//p[@class="events_tag"]/a/span/text()').extract():
            item['type'] = eventsummaryinfoview.xpath('//p[@class="events_tag"]/a/span/text()').extract()[0]
        else:
            item['type'] = ''
        if response.xpath('//div[@class="events_infotext"]/p/text()').extract():
            item['description'] = ' '.join(filter(lambda v: v, [tmp.strip() for tmp in response.xpath(
                '//div[@class="events_infotext"]/p/text()').extract()]))
        else:
            item['description'] = ''
        item['comments'] = []
        frmdata = {'page': '1', 'eventId': str(item['tid'])}
        url = 'http://you.ctrip.com/CommunitySite/Activity/EventDetail/EventReplyListOrCommentList'
        yield FormRequest(url, formdata=frmdata, method='POST',
                          meta={'item': item, 'page': 1}, callback=self.parse_comments)

    def parse_comments(self, response):
        item = response.meta['item']

        reply_boxes = response.xpath('//div[@class="reply_conbox"]')
        for node in reply_boxes:
            cid = node.xpath('.//@data-replyid').extract()[0]
            author = node.xpath('.//div/p/a[@class="user_name"]/text()').extract()[0]
            avatar = node.xpath('.//div/a/img/@src').extract()[0]
            comment = node.xpath('.//div/p[@class="replytext"]/text()').extract()[0]
            comment_item = {'cid': cid, 'author_avatar': avatar, 'author': author, 'comment': comment}
            item['comments'].append(comment_item)

        if not reply_boxes:
            # 没有评论, 可以返回item
            yield item
        else:
            # 尝试读取下一页
            meta = response.meta
            page = meta['page'] + 1
            form_data = {'page': str(page), 'eventId': str(item['tid'])}
            yield FormRequest(response.url, formdata=form_data, method='POST',
                              meta={'item': item, 'page': page}, callback=self.parse_comments)
