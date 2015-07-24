# coding=utf-8
import json
from urlparse import urljoin
import re
from datetime import datetime, timedelta

import scrapy
from scrapy.http import Request
from scrapy.selector import Selector

from andaman.items.mafengwo import MafengwoQuestion


__author__ = 'zephyre'


class MafengwoQaSpider(scrapy.Spider):
    name = 'mafengwo-qa'

    def parse(self, response):
        html_text = json.loads(response.body)['payload']['list_html']
        for href in Selector(text=html_text).xpath(
                '//li/div[@class="wen"]//div[@class="title"]/a[@href]/@href').extract():
            url = urljoin(response.url, href)
            yield Request(url=url, callback=self.parse_question)

    def start_requests(self):
        for start_idx in xrange(0, 500, 20):
            yield Request(url='http://www.mafengwo.cn/qa/ajax_pager.php?action=question_index&start=%d' % start_idx)

    def parse_question(self, response):
        # 抓取相关问题
        for related_href in response.selector.xpath(
                '//div[@class="q-relate"]/ul[@class="bd"]/li/a[@href]/@href').extract():
            url = urljoin(response.url, related_href)
            yield Request(url=url, callback=self.parse_question)

        tmp = response.selector.xpath('//div[@class="q-detail"]/div[@class="person"]/div[@class="avatar"]/a[@href]')
        user_href = tmp[0].xpath('./@href').extract()[0]
        m = re.search(r'/wenda/u/(\d+)', user_href)
        author_id = int(m.group(1))
        tmp = tmp[0].xpath('./img/@src').extract()[0]
        author_avatar = re.sub(r'\.head\.w\d+\.', '.', tmp)
        if author_avatar.endswith('pp48.gif'):
            author_avatar = ''
        author_name = response.selector.xpath(
            '//div[@class="q-content"]/div[@class="user-bar"]/a[@class="name"]/text()').extract()[0]

        title = response.selector.xpath('//div[@class="q-content"]/div[@class="q-title"]/h1/text()').extract()[0]
        contents = ''.join([tmp.strip() for tmp in response.selector.xpath(
            '//div[@class="q-content"]/div[@class="q-info"]/div[@class="q-desc"]/descendant-or-self::text()').extract()])
        tmp = response.selector.xpath(
            '//div[@class="q-content"]/div[@class="user-bar"]//span[@class="visit"]/text()').extract()[0]
        view_cnt = int(re.search(ur'(\d+)\s*浏览', tmp).group(1))

        time_str = response.selector.xpath(
            '//div[@class="q-content"]/div[@class="user-bar"]//span[@class="time"]/text()').extract()[0]
        tz_delta = timedelta(seconds=8 * 3600)
        timestamp = long((datetime.strptime(time_str, '%y/%m/%d %H:%M') - tz_delta - datetime.utcfromtimestamp(
            0)).total_seconds() * 1000)

        tmp = response.selector.xpath(
            '//div[@class="q-content"]/div[@class="user-bar"]/span[@class="fr"]/a[@href]/text()').extract()
        if tmp and tmp[0].strip():
            topic = tmp[0].strip()
        else:
            topic = ''

        raw_tags = response.selector.xpath(
            '//div[@class="q-content"]/div[@class="q-info"]/div[@class="q-tags"]/a[@class="a-tag"]/text()').extract()
        tags = [tmp.strip() for tmp in raw_tags if tmp.strip()]

        match = re.search(r'detail-(\d+)\.html', response.url)
        qid = int(match.group(1))

        item = MafengwoQuestion()
        item['qid'] = qid
        item['title'] = title
        item['author_nickname'] = author_name
        item['author_id'] = author_id
        item['author_avatar'] = author_avatar
        item['timestamp'] = timestamp
        item['topic'] = topic
        item['contents'] = contents
        item['tags'] = tags
        item['view_cnt'] = view_cnt

        yield item
