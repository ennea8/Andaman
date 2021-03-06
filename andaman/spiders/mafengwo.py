# coding=utf-8
import json
import re
from urlparse import urljoin

import scrapy
from scrapy.http import Request
from scrapy.selector import Selector

from andaman.items.jieban import JiebanItem
from andaman.items.qa import QAItem
from andaman.utils.html import html2text, parse_time

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

        q_item = self.retrive_question(response)
        yield q_item

        # 抓取回答
        qid = q_item['qid']
        page = 0
        page_size = 50
        url = 'http://www.mafengwo.cn/qa/ajax_pager.php?qid=%d&action=question_detail&start=%d' \
              % (qid, page * page_size)
        yield Request(url=url, callback=self.parse_answer_list, meta={'qid': qid, 'page': page, 'page_size': page_size})

    def retrive_question(self, response):
        """
        分析response，得到问题
        """
        tmp = response.selector.xpath('//div[@class="q-detail"]/div[@class="person"]/div[@class="avatar"]/a[@href]')
        try:
            user_href = tmp[0].xpath('./@href').extract()[0]
        except IndexError:
            self.logger.warning('Invalid response: %s' % response.url)
            self.logger.warning(response.body)
            raise
        m = re.search(r'/wenda/u/(\d+)', user_href)
        author_id = int(m.group(1))
        tmp = tmp[0].xpath('./img/@src').extract()[0]
        author_avatar = re.sub(r'\.head\.w\d+\.', '.', tmp)
        if author_avatar.endswith('pp48.gif'):
            author_avatar = None
        author_name = response.selector.xpath(
                '//div[@class="q-content"]/div[@class="user-bar"]/a[@class="name"]/text()').extract()[0]

        title = response.selector.xpath('//div[@class="q-content"]/div[@class="q-title"]/h1/text()').extract()[0]

        raw_contents = \
            response.selector.xpath('//div[@class="q-content"]/div[@class="q-info"]/div[@class="q-desc"]').extract()[0]
        contents = html2text(raw_contents)

        tmp = response.selector.xpath(
                '//div[@class="q-content"]/div[@class="user-bar"]//span[@class="visit"]/text()').extract()[0]
        view_cnt = int(re.search(ur'(\d+)\s*浏览', tmp).group(1))

        time_str = response.selector.xpath(
                '//div[@class="q-content"]/div[@class="user-bar"]//span[@class="time"]/text()').extract()[0]
        timestamp = parse_time(time_str)

        tmp = response.selector.xpath(
                '//div[@class="q-content"]/div[@class="user-bar"]/span[@class="fr"]/a[@href]/text()').extract()
        if tmp and tmp[0].strip():
            topic = tmp[0].strip()
        else:
            topic = None

        raw_tags = response.selector.xpath(
                '//div[@class="q-content"]/div[@class="q-info"]/div[@class="q-tags"]/a[@class="a-tag"]/text()').extract()
        tags = [tmp.strip() for tmp in raw_tags if tmp.strip()]

        match = re.search(r'detail-(\d+)\.html', response.url)
        qid = int(match.group(1))

        item = QAItem()
        item['source'] = 'mafengwo'
        item['type'] = 'question'
        item['qid'] = qid
        item['title'] = title
        item['author_nickname'] = author_name
        item['author_id'] = author_id
        if author_avatar:
            item['author_avatar'] = author_avatar
            item['file_urls'] = [author_avatar]
        item['timestamp'] = timestamp
        if topic:
            item['topic'] = topic
        item['contents'] = contents
        item['tags'] = tags
        item['view_cnt'] = view_cnt

        return item

    def parse_answer_list(self, response):
        meta = response.meta
        qid = meta['qid']
        page = meta['page']
        page_size = meta['page_size']

        sel = Selector(text=json.loads(response.body)['payload']['list_html'])
        answer_nodes = sel.xpath('//li[contains(@class, "answer-item")]')
        if not answer_nodes:
            return

        # 查找下一页
        if len(answer_nodes) == page_size:
            next_page = page + 1
            url = 'http://www.mafengwo.cn/qa/ajax_pager.php?qid=%d&action=question_detail&start=%d' \
                  % (qid, next_page * page_size)
            yield Request(url=url, callback=self.parse_answer_list,
                          meta={'qid': qid, 'page': next_page, 'page_size': page_size})

        for answer_node in sel.xpath('//li[contains(@class, "answer-item") and @data-aid]'):
            aid = int(answer_node.xpath('./@data-aid').extract()[0])

            author_node = answer_node.xpath('./div[@class="person"]/div[contains(@class, "avatar") and @data-uid]')[0]
            author_id = int(author_node.xpath('./@data-uid').extract()[0])
            tmp = author_node.xpath('./a/img/@src').extract()[0]
            author_avatar = re.sub(r'\.head\.w\d+\.', '.', tmp)
            if author_avatar.endswith('pp48.gif'):
                author_avatar = None

            content_node = answer_node.xpath('./div[contains(@class,"answer-content")]')[0]

            author_name = content_node.xpath('./div[@class="user-bar"]/a[@class="name"]/text()').extract()[0]

            time_str = content_node.xpath('./div[@class="user-bar"]//span[@class="time"]/text()').extract()[0]
            timestamp = parse_time(time_str)

            accepted = bool(answer_node.xpath('.//div[contains(@class,"answer-best")]'))

            raw_contents = content_node.xpath('.//dl/dd[@class="_j_answer_html"]').extract()[0]
            contents = html2text(raw_contents)

            try:
                vote_cnt = int(answer_node.xpath('.//a[@class="btn-zan"]/span/text()').extract()[0])
            except (IndexError, ValueError):
                self.logger.debug(u'Invalid vote count: %s' % answer_node.extract()[0])
                vote_cnt = 0

            item = QAItem()
            item['type'] = 'answer'
            item['source'] = 'mafengwo'
            item['qid'] = qid
            item['aid'] = aid
            item['author_nickname'] = author_name
            item['author_id'] = author_id
            if author_avatar:
                item['author_avatar'] = author_avatar
                item['file_urls'] = [author_avatar]
            item['timestamp'] = timestamp
            item['contents'] = contents
            item['vote_cnt'] = vote_cnt
            item['accepted'] = accepted

            yield item


class MafengwoJiebanSpider(scrapy.Spider):
    """
    抓取蚂蜂窝的结伴信息

    配置项目说明:
    * MAFENGWO_JIEBAN_PAGES: 抓取前多少页的结伴信息
    * MAFENGWO_SESSION_ID: 蚂蜂窝登录以后, cookie中需要指定PHPSESSID, 表示一个session
    * MAFENGWO_JOIN_EVENT: 是否报名以获取详细的联系信息?
    * MAFENGWO_USER_ID: 蚂蜂窝登录以后, 分配的用户ID
    """
    name = "mafengwo-jieban"
    allowed_domains = ["mafengwo.cn"]

    def start_requests(self):
        total_page = self.crawler.settings.getint('MAFENGWO_JIEBAN_PAGES', 10)
        session_id = self.crawler.settings.get('MAFENGWO_SESSION_ID')
        cookies = {'PHPSESSID': session_id} if session_id else {}
        for i in range(total_page):
            url = 'http://www.mafengwo.cn/together/ajax.php?act=getTogetherMore&flag=3&offset=%d&mddid=0&timeFlag=1' \
                  '&timestart=' % i
            yield scrapy.Request(url, cookies=cookies)

    def parse(self, response):
        data = json.loads(response.body)
        try:
            if data['error']['msg'] == 'login:required':
                self.logger.error('Login required, mission aborted.')
                return
        except ValueError:
            self.logger.error('Invalid response, mission aborted.')
            return
        except KeyError:
            pass

        hrefs = scrapy.Selector(text=data['data']['html']).xpath('//li/a/@href').extract()
        for href in hrefs:
            url = urljoin('http://www.mafengwo.cn/together/', href)
            yield scrapy.Request(url, callback=self.parse_dir_contents)

    @staticmethod
    def validate_comments_req(response):
        """
        获取评论列表的请求, 会返回一个JSON格式的数据. 通过这一点验证代理服务器是否生效
        :param response:
        :return:
        """
        status_code = getattr(response, 'status', 500)

        is_valid = 200 <= status_code < 400 and response.body.strip()
        if not is_valid:
            return False

        try:
            json.loads(response.body)
            return True
        except ValueError:
            return False

    def parse_dir_contents(self, response):
        tid = int(str(response.xpath('//script[1]/text()').re(r'"tid":\d+')[0])[6:])
        total = int(str(response.xpath('//script[1]/text()').re(r'"total":\d+')[0][8:])) / 10 + 1
        summary = response.xpath('//div[@class="summary"]')
        item = JiebanItem()
        item['source'] = 'mafengwo'
        item['title'] = response.xpath('//title/text()').extract()[0]

        item['start_time'] = summary.xpath('//div[@class="summary"]/ul/li[1]/span/text()').extract()[0].encode("UTF-8")[
                             15:]
        item['days'] = summary.xpath('//div[@class="summary"]/ul/li[2]/span/text()').extract()[0].encode("UTF-8")[9:]
        item['destination'] = summary.xpath('//div[@class="summary"]/ul/li[3]/span/text()').extract()[0].encode(
                "UTF-8")[12:].split("/")
        item['departure'] = summary.xpath('//div[@class="summary"]/ul/li[4]/span/text()').extract()[0].encode("UTF-8")[
                            12:]
        item['groupSize'] = summary.xpath('//div[@class="summary"]/ul/li[5]/span/text()').extract()[0].encode("UTF-8")[
                            15:]
        item['description'] = '\n'.join(filter(lambda v: v, [tmp.strip() for tmp in summary.xpath(
                '//div[@class="desc _j_description"]/text()').extract()])).encode("UTF-8")
        item['author_avatar'] = summary.xpath('//div[@class="sponsor clearfix"]/a/img/@src').extract()[0].encode(
                "UTF-8")
        item['comments'] = []
        item['tid'] = tid

        contact_info = self.extract_contact(response)
        if contact_info or not self.crawler.settings.getbool('MAFENGWO_JOIN_EVENT', False):
            item['contact'] = contact_info

            # 不用再请求联系人信息, 直接开始获取评论
            url = 'http://www.mafengwo.cn/together/ajax.php?act=moreComment&page=%d&tid=%d' % (0, tid)
            yield scrapy.Request(url, meta={'item': item, 'total': total, 'page': 0}, callback=self.parse_comments)
        else:
            # 构造报名请求
            uid = str(self.crawler.settings.get('MAFENGWO_USER_ID'))
            tel = '13800138000'
            form_data = {'act': 'saveAddUser', 'phone': tel, 'gender': '1', 'tid': str(tid), 'uid': uid}
            url = 'http://www.mafengwo.cn/together/ajax.php'
            yield scrapy.FormRequest(url, formdata=form_data, method='POST', dont_filter=True,
                                     meta={'item': item, 'total': total}, callback=self.parse_contact)

    @staticmethod
    def extract_contact(response):
        """
        尝试从response中提取联系信息
        :param response:
        :return:
        """
        tmp = response.xpath('//div[@class="contact _j_contact"]'
                             '/span[not(contains(@class,"invisible"))]'
                             '/descendant-or-self::text()').extract()
        return ' '.join(filter(lambda v: v, [t.strip() for t in tmp]))

    def parse_contact(self, response):
        """
        解析报名后返回的联系信息
        :param response:
        :return:
        """
        meta = response.meta
        item = meta['item']
        total = meta['total']
        tid = item['tid']

        # TODO 解析返回的联系信息
        try:
            tmp = Selector(text=json.loads(response.body)['data']['html']) \
                .xpath('//descendant-or-self::text()').extract()
            contact_info = ' '.join(filter(lambda v: v, [t.strip() for t in tmp]))
            if contact_info:
                item['contact'] = contact_info
        except ValueError:
            pass
        contact_info = self.extract_contact(response)
        if contact_info:
            item['contact'] = contact_info

        url = 'http://www.mafengwo.cn/together/ajax.php?act=moreComment&page=%d&tid=%d' % (0, tid)
        yield scrapy.Request(url, meta={'item': item, 'total': total, 'page': 0}, callback=self.parse_comments)

    # def parse_contact(self, response):
    #     uid = str(self.crawler.settings.get('MAFENGWO_USER_ID', ''))
    #     tel = '13800138000'
    #     frmdata = {'act': 'saveAddUser', 'phone': tel, 'gender': '1',
    #                'tid': str(response.meta['item']['tid']), 'uid': uid}
    #     url = 'http://www.mafengwo.cn/together/ajax.php'
    #     yield scrapy.FormRequest(url, formdata=frmdata, method='POST', dont_filter=True,
    #                              meta={'item': response.meta['item'], 'page': 0, 'total': response.meta['total']},
    #                              callback=self.parse_comments)

    def parse_comments(self, response):
        item = response.meta['item']
        page = response.meta['page'] + 1
        body = scrapy.Selector(text=json.loads(response.body)['data']['html'])
        if body.extract() != '<html></html>':
            for node in body.xpath('//div[@class="vc_comment"]'):
                try:
                    author_avatar = node.xpath('.//div[@class= "avatar"]/a/img/@src').extract()[0].encode("UTF-8")
                    author = node.xpath('.//a[@class="comm_name"]/text()').extract()[0].encode("UTF-8")
                    cid = int(node.xpath('.//div[@class="comm_reply"]/a/@data-cid').extract()[0].encode("UTF-8"))
                    comment = '\n'.join(
                            filter(lambda v: v, [tmp.strip() for tmp in node.xpath('.//p/text()').extract()])).encode(
                            "UTF-8")
                    comment_item = {'cid': cid, 'author_avatar': author_avatar, 'author': author, 'comment': comment}
                    item['comments'].append(comment_item)
                except IndexError:
                    self.logger.warning('Unable to extract comment from: %s' % response.url)
        if page <= response.meta['total']:
            url = 'http://www.mafengwo.cn/together/ajax.php?act=moreComment&page=%d&tid=%d' % (page, item['tid'])
            yield scrapy.Request(url, callback=self.parse_comments,
                                 meta={'item': item, 'page': page, 'total': response.meta['total'],
                                       'dyno_proxy_validator': self.validate_comments_req})
        else:
            yield item
