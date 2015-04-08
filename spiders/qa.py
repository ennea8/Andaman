# coding=utf-8
import re
import json

from scrapy import Item, Field, Request, Selector,log

from spiders import AizouCrawlSpider, AizouPipeline
from utils.database import get_mongodb


__author__ = 'lxf'


class QaItem(Item):
    """
    携程问答
    """
    type = Field()
    data = Field()


class CtripQA(AizouCrawlSpider):
    """
    携程问答
    """
    name = 'ctrip-qa'
    uuid = '97168ee6-6006-4eba-ac9a-7c8223275a'

    def start_requests(self):
        page_idx = 1
        url = 'http://you.ctrip.com'
        data = {'page_idx': page_idx, 'domain': url}
        yield Request(url='%s/asks' % (url), callback=self.parse,
                      meta={'data': data})

    def parse(self, response):
        tmp_data = response.meta['data']
        # 翻页
        page_idx = tmp_data['page_idx']
        # 域名
        domain = tmp_data['domain']
        sel = Selector(response)
        # 页面提问列表
        asks_list = sel.xpath('//div[@class="main"]/div[@class="asklist_con"]/ul[@class="asklist"]/li')
        if asks_list:
            for node in asks_list:
                tmp_url = node.xpath('./@href').extract()
                if tmp_url:
                    url = '%s%s' % (domain, tmp_url[0])
                    # 具体解析页面
                    yield Request(url=url, callback=self.parse_detail, meta={'sub_url': tmp_url[0]})
                else:
                    continue

            page_idx += 1
            data = {'domain': domain, 'page_idx': page_idx}
            # 到下一页
            yield Request(url='%s/asks/p%d' % (domain, page_idx), callback=self.parse,
                          meta={'data': data})
        else:
            # 解析结束
            return

    @staticmethod
    def parse_detail(response):
        sel = Selector(response)
        q_item = QaItem()
        sub_url = response.meta['sub_url']
        question = sel.xpath('//div[@class="detailmain"]/div[@class="detailmain_top"]').extract()

        # 问题的id
        q_id = sel.xpath('//div[@class="detailmain"]//div[@class="ask_infoline cf"]//li/a/@data-shareid').extract()
        if q_id:
            q_id = q_id[0]
        else:
            return
        tmp_data = {'q_id': q_id, 'body': question[0], 'sub_url': sub_url}
        q_item['type'] = 'question'
        q_item['data'] = tmp_data
        yield q_item

        # 抽取答案 
        best_anwser = sel.xpath('//div[@class="detailmain"]/div[@class="bestanswer_con"]').extract()
        other_answer_list = sel.xpath('//div[@class="detailmain"]/div[@id="divAskReplyListContent"]//li')
        if best_anwser:
            a_id = sel.xpath(
                '//div[@class="detailmain"]/div[@class="bestanswer_con"]/div[@class="answer_box cf"]/@data-answerid').extract()
            a_item = QaItem()
            tmp_data = {'rec': True, 'q_id': q_id, 'body': best_anwser[0], 'a_id': a_id[0]}
            a_item['type'] = 'answer'
            a_item['data'] = tmp_data
            yield a_item
        if other_answer_list:
            for node in other_answer_list:
                a_id = node.xpath('./div[@class="answer_box cf"]/@data-answerid').extract()
                if a_id:
                    a_id = a_id[0]
                else:
                    continue
                a_item = QaItem()
                tmp_data = {'q_id': q_id, 'body': node.extract(), 'a_id': a_id}
                a_item['type'] = 'answer'
                a_item['data'] = tmp_data
                yield a_item
        else:
            return


class CtripQAPipeline(AizouPipeline):
    spiders = [CtripQA.name]
    spiders_uuid = [CtripQA.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        # 数据库授权
        if item_type == 'question':
            col = get_mongodb('raw', 'CtripQuestion', 'mongo-raw')
            col.update({'q_id': data['q_id']}, {'$set': data}, upsert=True)
        else:
            col = get_mongodb('raw', 'CtripAnswer', 'mongo-raw')
            col.update({'a_id': data['a_id']}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
        return item


class QunarQA(AizouCrawlSpider):
    """
    去哪问答数据抓取
    """
    name = 'qunar-qa'
    uuid = '58df13df-b134-4164-9e71-cdac2a19b314'

    def start_requests(self):
        page_idx = 1
        domain = 'http://travel.qunar.com/bbs/'
        url = 'http://travel.qunar.com/bbs/forum.php?mod=forumdisplay&fid=54'
        data = {'page_idx': page_idx, 'domain': domain}
        yield Request(url=url, callback=self.parse, meta={'data': data})

    def parse(self, response):
        tmp_data = response.meta['data']
        # 翻页
        page_idx = tmp_data['page_idx']

        # log.msg('page_idx:%d' % page_idx, level=log.INFO)
        # 开始的url
        domain = tmp_data['domain']
        sel = Selector(response)
        # 页面提问列表
        asks_list = sel.xpath('//table[@id="threadlisttableid"]/tbody')
        if page_idx <= 100:  # 100页以内有效
            for node in asks_list[1:]:
                # 获取url
                tmp_url = node.xpath('.//a[@class="s xst"]/@href').extract()
                # 获取title_id进行数据关联
                if tmp_url:
                    tmp_ques_id = node.xpath('./@id').extract()
                    match = re.search('\d+', tmp_ques_id[0])
                    q_id = match.group()
                    subpage_url = '%s%s' % (domain, tmp_url[0])
                    # 具体解析页面,可能翻页
                    subpage_idx = 1
                    sub_data = {'subpage_idx': subpage_idx, 'subpage_url': subpage_url, 'q_id': q_id}
                    yield Request(url=subpage_url, callback=self.parse_detail,
                                  meta={'sub_data': sub_data})
                else:
                    continue

            page_idx += 1
            data = {'domain': domain, 'page_idx': page_idx}
            # 到下一页
            yield Request(url='http://travel.qunar.com/bbs/forum.php?mod=forumdisplay&fid=54&page=%d' % (page_idx),
                          callback=self.parse,
                          meta={'data': data})
        else:
            # 解析结束
            return

    # 问答解析
    def parse_detail(self, response):
        sel = Selector(response)
        sub_data = response.meta['sub_data']
        # 翻页
        subpage_idx = sub_data['subpage_idx']
        # url
        subpage_url = sub_data['subpage_url']
        # title_id
        q_id = sub_data['q_id']
        post_list = sel.xpath('//div[@id="postlist"]/div')
        # 翻页按钮
        page_total = sel.xpath('//div[@id="pgt"]/div[@class="pgt"]/div[@class="pg"]/label/span/@title').extract()

        if page_total:  # 说明可以翻页
            # 获取总的页数
            match = re.search(r'\d+', page_total[0])
            page_total = match.group()
            page_total = int(page_total)
            if subpage_idx <= page_total:
                # 抓取帖子
                if subpage_idx == 1:
                    title = sel.xpath('//span[@id="thread_subject"]').extract()
                    # 将1页1楼存放在问题中
                    data = {}
                    tmp_node = post_list[0]
                    tmp_post_id = tmp_node.xpath('./@id').extract()
                    match = re.search(r'\d+', tmp_post_id[0])
                    if match:
                        post_id = match.group()
                        data['post_id'] = post_id
                        data['title'] = title
                        data['body'] = tmp_node.extract()
                        data['q_id'] = q_id
                    post_item = QaItem()
                    post_item['data'] = data
                    post_item['type'] = 'question'
                    yield post_item
                    # 处理其余的node
                    for node in post_list[1:-1]:
                        data = {}
                        tmp_post_id = node.xpath('./@id').extract()
                        match = re.search(r'\d+', tmp_post_id[0])
                        if match:
                            post_id = match.group()
                            data['post_id'] = post_id
                            data['body'] = node.extract()
                            data['q_id'] = q_id
                            post_item = QaItem()
                            post_item['data'] = data
                            post_item['type'] = 'answer'
                            yield post_item
                        else:
                            continue

                else:
                    # 处理其余页码的node
                    for node in post_list[0:-1]:
                        data = {}
                        tmp_post_id = node.xpath('./@id').extract()
                        match = re.search(r'\d+', tmp_post_id[0])
                        if match:
                            post_id = match.group()
                            data['post_id'] = post_id
                            data['body'] = node.extract()
                            data['q_id'] = q_id
                            post_item = QaItem()
                            post_item['data'] = data
                            post_item['type'] = 'answer'
                            yield post_item
                        else:
                            continue
            else:
                return
            # 翻到下一页
            subpage_idx += 1
            sub_data = {'subpage_idx': subpage_idx, 'subpage_url': subpage_url, 'q_id': q_id}
            tmp_url = '%s&page=%d' % (subpage_url, subpage_idx)
            yield Request(url=tmp_url,
                          callback=self.parse_detail,
                          meta={'sub_data': sub_data})

        # 不可以翻页，解析第一页
        else:
            # 将1页1楼存放在问题中
            data = {}
            title = sel.xpath('//span[@id="thread_subject"]').extract()
            tmp_node = post_list[0]
            tmp_post_id = tmp_node.xpath('./@id').extract()
            match = re.search(r'\d+', tmp_post_id[0])
            if match:
                post_id = match.group()
                data['post_id'] = post_id
                data['title'] = title[0]
                data['body'] = tmp_node.extract()
                data['q_id'] = q_id
            post_item = QaItem()
            post_item['data'] = data
            post_item['type'] = 'question'
            yield post_item
            # 处理其余的node
            for node in post_list[1:-1]:
                data = {}
                tmp_post_id = node.xpath('./@id').extract()
                match = re.search(r'\d+', tmp_post_id[0])
                if match:
                    post_id = match.group()
                    data['post_id'] = post_id
                    data['body'] = node.extract()
                    data['q_id'] = q_id
                    post_item = QaItem()
                    post_item['data'] = data
                    post_item['type'] = 'answer'
                    yield post_item
                else:
                    continue


class QunarQAPipeline(AizouPipeline):
    spiders = [QunarQA.name]
    spiders_uuid = [QunarQA.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        # 数据库授权
        if item_type == 'question':
            col = get_mongodb('raw', 'QunarQuestion', 'mongo-raw')
            col.update({'post_id': data['post_id']}, {'$set': data}, upsert=True)
        else:
            col = get_mongodb('raw', 'QunarAnswer', 'mongo-raw')
            col.update({'post_id': data['post_id']}, {'$set': data}, upsert=True)
        return item



__author__ = 'bxm'

class MafengwoQA(AizouCrawlSpider):
    """
    抓取蚂蜂窝的问答
    """

    name = 'mafengwo-qa'
    uuid = '6e98b3d3-1d7b-422a-ab61-215b642f7a2d'

    def start_requests(self):
        url = 'http://www.mafengwo.cn/qa/ajax_pager.php?action=question_index&start=0'
        data = {'start': 0, 'domain': 'http://www.mafengwo.cn'}
        yield Request(url=url, callback=self.parse, meta=data)

    def json_to_dic(self, response):
        try:
            return json.loads(response.body)
        except (ValueError, KeyError):
            return None


    def parse(self, response):
        domain = response.meta['domain']
        start = response.meta['start']

        # 从返回的json中提取html,
        page_dict = self.json_to_dic(response)
        page_html = ''
        if page_dict:
            page_html = page_dict['payload']['list_html']
        sel = Selector(text=page_html)
        # 得到问题链接的列表
        ask_list = sel.xpath('//div[@class="title"]/a/@href').extract()

        for url_suffix in ask_list:
            if url_suffix:
                url_request = '%s%s' % (domain, url_suffix)
                #log.msg(url_request)
                yield Request(url=url_request, callback=self.parse_question)
            else:
                continue

       # 每次ajax请求返回20个问题
        start += 20
        #总共500个问题
        if start >= 500:
            return
        url_response = re.sub(r'\d+', '%d' % start, response.url)
        data = {'start': start, 'domain': domain}
        yield Request(url=url_response, callback=self.parse, meta=data)


    def parse_question(self, response):
        """
        从问答中提取问题，如：http://www.mafengwo.cn/wenda/detail-207723.html
        """
        sel = Selector(response)
        q_id = sel.xpath('//div[@class="wrapper"]/@data-qid').extract()
        question = sel.xpath('//div[@class="q-detail"]').extract()
        q_item = QaItem()
        # log.msg(u'请求question')
        q_item['type'] = 'question'
        if q_id and question:
            q_item['data'] = {'q_id': q_id[0], 'body': question[0]}
            yield q_item
        else:
            return

        url_request = "http://www.mafengwo.cn/qa/ajax_pager.php?_uid=0&qid=%s&action=question_detail" % q_id[0]
        #url_request='http://www.mafengwo.cn/qa/ajax_pager.php?_uid=0&qid=2319020&action=question_detail'
        start = 0
        yield Request(url=url_request, callback=self.parse_answer, meta={'start': start, 'domain': url_request})

    def parse_answer(self, response):
        """
        解析通过ajax请求得到的问题答案，如：http://www.mafengwo.cn/qa/ajax_pager.php?_uid=0&qid=207723&action=
        question_detail&start=0
        """
        start = response.meta['start']
        domain = response.meta['domain']


        a_dict = self.json_to_dic(response)
        a_html = ''
        a_total = 0
        if a_dict:
            a_html = a_dict['payload']['list_html']
            # 答案数量
            a_total = a_dict['payload']['total']
        sel = Selector(text=a_html)
        answer_list = sel.xpath('//li[@class="answer-item clearfix _j_answer_item"]').extract()
        q_id = sel.xpath('//div[@class="share-pop _j_share_pop"]/@data-qid').extract()
        if a_total==0 or not q_id:
            return
        #log.msg('q_id%s:anwsers%s'%(q_id[0],a_total))

        for answer in answer_list:
            a_id = Selector(text=answer).xpath('//li/@data-aid').extract()
            a_best = Selector(text=answer).xpath('//div[@class="answer-content answer-best"]').extract()
            # a_item=QaItem()放循环外面将只返回一个
            a_item = QaItem()
            a_item['type'] = 'answer'
            # 是否最佳答案
            if a_best:
                a_item['data'] = {'rec': True, 'a_id': a_id[0], 'q_id': q_id[0], 'body': answer}
            else:
                a_item['data'] = {'a_id': a_id[0], 'q_id': q_id[0], 'body': answer}
            yield a_item

        # 每次请求返回50个答案
        start += 50
        # if start > a_total or start>=100:
        if start > a_total:
            return
        yield Request(url='%s&start=%s' % (domain, start), callback=self.parse_answer,
                      meta={'start': start, 'domain': domain})


class MafengwoQAPipeline(AizouPipeline):
    """
    蚂蜂窝数据进入数据库
    """
    spiders = [MafengwoQA.name]
    spiders_uuid = [MafengwoQA.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        if item_type == 'question':
            col = get_mongodb('raw', 'MafengwoQuestion', 'mongo-raw')
            col.update({'q_id': data['q_id']}, {'$set': data}, upsert=True)
        else:
            col = get_mongodb('raw', 'MafengwoAnswer', 'mongo-raw')
            col.update({'a_id': data['a_id']}, {'$set': data}, upsert=True)
        return item


