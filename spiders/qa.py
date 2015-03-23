# coding=utf-8
import re
from scrapy import Item, Field, Request, Selector, log
from spiders import AizouCrawlSpider, AizouPipeline
from utils.database import get_mongodb

__author__ = 'lxf'


class QaItem(Item):
    """
    携程问答
    """
    type = Field()
    data = Field()


class CtripDataProc(AizouCrawlSpider):
    """
    携程问答
    """
    name = 'ctrip_qa'
    uuid = 'A0D57DE3-1C3C-79D6-8BF4-1CCDEBB2FBD9'

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
        #log.msg('page_idx:%d' % page_idx, level=log.INFO)
        # 域名
        domain = tmp_data['domain']
        sel = Selector(response)
        # 页面提问列表
        asks_list = sel.xpath('//div[@class="main"]/div[@class="asklist_con"]/ul[@class="asklist"]/li')
        if asks_list:
            for node in asks_list:
                tmp_url = node.xpath('./@href').extract()
                # log.msg('url:%s' % tmp_url)
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

    def parse_detail(self, response):
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
            # rec----------
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


class CtripDataProcPipeline(AizouPipeline):
    spiders = [CtripDataProc.name]
    spiders_uuid = [CtripDataProc.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        # 数据库授权
        if item_type == 'question':
            col = get_mongodb('raw_faq', 'CtripQuestion', 'mongo-raw')
            col.update({'q_id': data['q_id']}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
        else:
            col = get_mongodb('raw_faq', 'CtripAnswer', 'mongo-raw')
            col.update({'a_id': data['a_id']}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
        return item


class QunarDataProc(AizouCrawlSpider):
    """
    去哪问答数据抓取
    """
    name = 'qunar-qa'
    uuid = 'F2C23067-2BB7-5ED5-5546-D3F6E95C7255'

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

        #log.msg('page_idx:%d' % page_idx, level=log.INFO)
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
            # 如果subpage_idx>page_total?
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


class QunarDataProcPipeline(AizouPipeline):
    spiders = [QunarDataProc.name]
    spiders_uuid = [QunarDataProc.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = item['type']

        # 数据库授权
        if item_type == 'question':
            col = get_mongodb('raw_faq', 'QunarQuestion', 'mongo-raw')
            col.update({'post_id': data['post_id']}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
        else:
            col = get_mongodb('raw_faq', 'QunarAnswer', 'mongo-raw')
            col.update({'post_id': data['post_id']}, {'$set': data}, upsert=True)
            # log.msg('note_id:%s' % data['note_id'], level=log.INFO)
        return item





