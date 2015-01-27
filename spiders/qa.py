# coding=utf-8
from scrapy import Item, Field, Request, Selector, log
from spiders import AizouCrawlSpider, AizouPipeline
from utils import get_mongodb

__author__ = 'lxf'


class CtripAsksItem(Item):
    """
    携程问答
    """
    type = Field()
    data = Field()


class CtripDataProc(AizouCrawlSpider):
    """
    携程问答
    """
    name = 'ctrip_asks'
    uuid = 'A0D57DE3-1C3C-79D6-8BF4-1CCDEBB2FBD9'

    def start_requests(self):
        # col = get_mongodb('raw_faq', 'CtripAnswer', 'mongo-raw')
        # col.find_one({'id': 'a'})
        page_idx = 1
        url = 'http://you.ctrip.com'
        data = {'page_idx': page_idx, 'domain': url}
        yield Request(url='%s/asks' % (url), callback=self.parse,
                      meta={'data': data})

    def parse(self, response):
        tmp_data = response.meta['data']
        # 翻页
        page_idx = tmp_data['page_idx']
        log.msg('page_idx:%d' % page_idx, level=log.INFO)
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
        q_item = CtripAsksItem()
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
            a_item = CtripAsksItem()
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
                a_item = CtripAsksItem()
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





