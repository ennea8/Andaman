# coding=utf-8
from urlparse import urljoin
import re

import scrapy
from scrapy.http import FormRequest, Request

from andaman.items.qa import QAItem
from andaman.utils.html import html2text, parse_time


__author__ = 'zephyre'


class QyerQaSpider(scrapy.Spider):
    name = 'qyer-qa'

    def start_requests(self):
        url = 'http://ask.qyer.com/ajax.php'
        page_size = 10

        settings = self.crawler.settings

        # 抓取前面多少页
        pages_cnt = settings.getint('QYER_QA_PAGES_CNT', 500)

        # 问题的种类。0: 最新问题 1: 热门问题 2: 待回答问题
        qa_type = settings.getint('QYER_QA_TYPE', 0)

        for page in xrange(pages_cnt):
            yield FormRequest(url=url,
                              formdata={'action': 'indexmore', 'start': str(page * page_size), 'type': str(qa_type),
                                        'from': str(0)})

    def parse(self, response):
        sel = response.selector
        for href in sel.xpath('//div[contains(@class,"ask_item_main_item_list")]//*[@class='
                              '"ask_item_main_item_list_title"]/a[@href]/@href').extract():
            yield Request(url=urljoin(response.url, href), callback=self.parse_question)

    def parse_question(self, response):
        sel = response.selector

        # 相关问题
        for href in sel.xpath('//div[contains(@class,"ask_sidebar")]//div[contains(@class,"ask_detail_do")]'
                              '/ul[contains(@class,"mt5")]/li//a[@title and @href]/@href').extract():
            yield Request(url=urljoin(response.url, href), callback=self.parse_question)

        qid = int(re.search(r'question/(\d+)\.html', response.url).group(1))

        # 回答
        yield Request(url='http://ask.qyer.com/index.php?action=ajaxanswer&qid=%d&orderway=use&page=1' % qid,
                      callback=self.parse_answers, meta={'qid': qid})

        try:
            q_details = sel.xpath('//div[contains(@class,"ask_item_main")]//div[contains(@class,'
                                  '"ask_detail_item")]')[0]
        except IndexError:
            self.logger.warning('Invalid response: %s, body: %s' % (response.url, response.body))
            raise
        user_url = urljoin(response.url,
                           q_details.xpath('./div[contains(@class,"ui_headPort")]/a[@href]/@href').extract()[0])

        q_contents = q_details.xpath('./div[@class="ask_detail_content"]')[0]

        title = html2text(q_contents.xpath('./*[contains(@class,"ask_detail_content_title")]').extract()[0])

        try:
            raw_contents = q_contents.xpath('./div[contains(@class,"ask_detail_content_text")]').extract()[0]
        except IndexError:
            self.logger.warning('Invalid response: %s, body: %s' % (response.url, response.body))
            raise
        contents = html2text(raw_contents)

        tags = [html2text(tmp) for tmp in q_contents.xpath(
            './div[contains(@class,"ask_detail_content_tag")]/a[@href and @class="ask_tag"]').extract()]

        time_str = q_contents.xpath('./div[contains(@class,"mt10")]//p[contains(@class,"fl") and '
                                    'contains(@class,"asker")]/span/text()').extract()[0]
        timestamp = self._get_timestamp(time_str)

        tmp = q_contents.xpath(
            './div[contains(@class,"mt10")]/span[contains(@class,"fl")]/a[@title and @href]/@title').extract()
        if tmp:
            topic = tmp[0]
        else:
            topic = None

        item = QAItem()
        item['type'] = 'question'
        item['source'] = 'qyer'
        item['qid'] = qid
        item['title'] = title
        if topic:
            item['topic'] = topic
        item['contents'] = contents
        item['tags'] = tags
        item['timestamp'] = timestamp

        yield Request(url=user_url, meta={'item': item}, callback=self.parse_user)

    def parse_answers(self, response):
        sel = response.selector
        qid = response.meta['qid']
        for node in sel.xpath('//div[contains(@class,"ask_detail_comment")]/div[contains(@class,"jsanswerbox")]'):
            aid = int(node.xpath('.//a[@href and contains(@class,"jsjubaoanswer") and @value]/@value').extract()[0])
            user_node = node.xpath('./div[@class="mod_discuss_face"]/div[@class="ui_headPort" and @alt]')[0]
            author_id = user_node.xpath('./@alt').extract()[0]
            avatar_node = user_node.xpath('./a[@href]/img[@src and @alt]')[0]

            author_avatar = self._get_avatar(avatar_node.xpath('./@src').extract()[0])
            author_nickname = avatar_node.xpath('./@alt').extract()[0]

            time_str = node.xpath('.//div[@class="jsanswercontent"]/a[1]/following-sibling::node()').extract()[0]
            timestamp = self._get_timestamp(time_str)

            contents = html2text(node.xpath('.//div[contains(@class,"mod_discuss_box_text")]').extract()[0])

            vote_cnt = int(node.xpath('.//a[contains(@class,"jsaskansweruseful")]/span/text()').extract()[0])

            item = QAItem()
            item['type'] = 'answer'
            item['source'] = 'qyer'
            item['qid'] = qid
            item['aid'] = aid
            item['author_nickname'] = author_nickname
            item['author_id'] = author_id
            if author_avatar:
                item['author_avatar'] = author_avatar
                item['file_urls'] = [author_avatar]
            item['timestamp'] = timestamp
            item['vote_cnt'] = vote_cnt
            item['contents'] = contents

            yield item

    @staticmethod
    def _get_avatar(img_url):
        # 这些是系统默认的头像
        if re.search(r'avatar/middle\d+\.png', img_url):
            return

        match = re.search(r'(.+)_avatar_(middle|big)\.(\w+).*?$', img_url)
        return '%s_avatar_big.%s' % (match.group(1), match.group(3)) if match else None

    @staticmethod
    def _get_timestamp(time_str):
        time_str = re.search(r'\s*\|(.+)', time_str).group(1).strip()
        return parse_time(time_str)

    def parse_user(self, response):
        item = response.meta['item']

        item['author_id'] = int(re.search(r'/u/(\d+)$', response.url).group(1))

        sel = response.selector

        try:
            img_url = sel.xpath('//div[@class="face"]/div[@class="img"]/img[@src]/@src').extract()[0]
        except IndexError:
            self.logger.warning('Invalid response: %s, body: %s' % (response.url, response.body))
            raise
        avatar = self._get_avatar(img_url)
        if avatar:
            item['author_avatar'] = avatar
            item['file_urls'] = [avatar]

        item['author_nickname'] = sel.xpath(
            '//div[@class="infos"]/*[@class="name"]/*[@data-bn-ipg="usercenter-username"]/text()').extract()[0]

        yield item

