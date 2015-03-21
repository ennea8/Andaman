# coding=utf-8

import copy
from scrapy import Item, Field, Request, Selector, log
from spiders import AizouCrawlSpider, AizouPipeline
import datetime
from utils.database import get_mongodb
import hashlib
from lxml import etree


class Ly(object):
    def __init__(self, *args, **kwargs):
        self.version = "20111128102912"  # 服务版本号,长期不变
        self.url = "http://tcopenapi.17usoft.com/handlers/scenery/queryhandler.ashx"  # 正式接口，非测试接口
        self.accountId = "7d9cfec6-0175-419e-9943-d546ff73dec0"  # 账号
        self.accountKey = "fd2c241e5282781b"  # 密码
        # self.serviceName = ""  # 接口名字
        self.reqTime = ""  # 时间戳 2014-04-09 09:55:07.020
        self.api = {
            'scenerylist': 'scenerylist',
            'scenerydetail': 'scenerydetail',
            'scenerytrafficinfo': 'scenerytrafficinfo',
            'sceneryimagelist': 'sceneryimagelist',
            'nearbyscenery': 'nearbyscenery',
            'sceneryprice': 'GetSceneryPrice',
            'pricecalendar': 'GetPriceCalendar'
        }

    def scenerylist(self):
        """
        获得景点列表
        :return:
        """
        return "GetSceneryList"

    def get_digital_sign(self, service):
        version = 'Version=' + self.version
        service_name = 'ServiceName=' + service
        req_time = 'ReqTime=' + self.reqTime
        account_id = 'AccountID=' + self.accountId
        sorted_array = self.bubblesort([account_id, req_time, service_name, version])
        m = hashlib.md5()
        m.update('&'.join(sorted_array) + self.accountKey)
        return m.hexdigest()

    def bubblesort(self, origin_array):
        return origin_array

    def update_req_time(self):
        self.reqTime = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S.000')

    def assemble_xml_header(self, service):
        self.update_req_time()
        req_header = ''
        req_header += '<header>'
        req_header += '<version>' + self.version + '</version>'
        req_header += '<accountID>' + self.accountId + '</accountID>'
        req_header += '<serviceName>' + service + '</serviceName>'
        req_header += '<digitalSign>' + self.get_digital_sign(service) + '</digitalSign>'
        req_header += '<reqTime>' + self.reqTime + '</reqTime>'
        req_header += '</header>'
        return req_header


    def assemble_xml_body(self, query_obj):
        req_body = ''
        req_body += '<body>'
        for key, value in query_obj.items():
            req_body += '<' + key + '>' + str(value) + '</' + key + '>'
        req_body += '</body>'
        return req_body


    def assemble_req_xml(self, service, query_obj):
        req_xml = ''
        req_xml += "<?xml version='1.0' encoding='utf-8' standalone='yes'?>"
        req_xml += '<request>'
        req_xml += self.assemble_xml_header(service)
        req_xml += self.assemble_xml_body(query_obj)
        req_xml += '</request>'
        return req_xml


    def crawl_vs(self):
        """
        crawl vs with ly api through city iteration
        :return:
        """
        conn = get_mongodb('raw_ly', 'ViewSpot', 'mongo-raw')
        city_list = self.crawl_city()
        self.logger.info('-=-=-=-=length: %s' % len(city_list))
        for ct in city_list:
            def func(city=ct):
                self.logger.info('================%s==============' % city)
                if int(city['location_id']) <= 35:
                    query_obj = {'clientIp': '127.0.0.1', 'provinceId': int(city['location_id'])}
                elif int(city['location_id']) <= 404:
                    query_obj = {'clientIp': '127.0.0.1', 'cityId': int(city['location_id'])}
                else:
                    query_obj = {'clientIp': '127.0.0.1', 'countryId': int(city['location_id'])}
                raw_xml = self.scenerylist().send_request(query_obj)
                node = etree.fromstring(raw_xml)
                responce_code = node.xpath('//rspCode/text()')[0]
                if '0000' == str(responce_code):
                    total_page = node.xpath('//sceneryList')[0].attrib['totalPage']
                    for page in xrange(int(total_page)):
                        temp_query = copy.deepcopy(query_obj)
                        temp_query['page'] = page + 1
                        raw_xml = self.scenerylist().send_request(temp_query)
                        node = etree.fromstring(raw_xml)
                        vs_nodes = node.xpath('//sceneryName')
                        for vs in vs_nodes:
                            name = vs.text.encode('utf-8')
                            ly_id = int(vs.xpath('../sceneryId')[0].text)
                            self.logger.info('----%s-----%s' % (name, ly_id))
                            conn.update({'lyId': ly_id}, {'$set': {'lyId': ly_id, 'lyName': name}}, upsert=True)

            self.add_task(func)


class Ticket(Item):
    # DUCK TYPING
    name = 'ticket'
    pid = Field()
    lyId = Field()
    info = Field()
    stock_list = Field()

class TicketDelete(Item):
    name = 'ticket-delete'
    id_list = Field()
    ly_id = Field()

class VsTicketInfo(Item):
    name = 'vs-ticket-info'
    info = Field()
    ly_id = Field()

class LyViewspotSpider(AizouCrawlSpider, Ly):
    """
    同城
    """

    name = 'ly-vs-info'
    uuid = 'a513791e-b26c-11e4-a71e-12e3f512a338'

    def __init__(self, *a, **kw):
        AizouCrawlSpider.__init__(self, *a, **kw)
        Ly.__init__(self, *a, **kw)
        self.date = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d')

    def start_requests(self):
        conn = get_mongodb('raw_ly', 'ViewSpot', profile='mongo-raw')
        for entry in conn.find({'crawl': False}, {'lyId': 1, 'lyName': 1}):
            scenery_id = int(entry['lyId'])
            request_body = {'clientIp': '127.0.0.1', 'sceneryIds': scenery_id, 'payType': 0}
            request_xml = self.assemble_req_xml(self.api['sceneryprice'], request_body)
            yield Request(
                url=self.url,
                method='POST',
                body=request_xml,
                headers={'Content-Type': 'application/x-www-form-urlencoded', 'X-Requested-With': 'XMLHttpRequest'},
                callback=self.parse_ticket_types,
                meta={'lyId': scenery_id})

    def parse_ticket_types(self, response):
        raw_xml = response.body
        ly_id = response.meta['lyId']

        root = etree.fromstring(raw_xml)
        ticket_list = []
        ticket_id_list = []
        for node in root.xpath('//policyId'):
            temp_obj = {}
            temp_obj['policyId'] = int(node.text)
            ticket_id_list.append(node.text)
            temp_obj['policyName'] = node.xpath('..//policyName')[0].text
            temp_obj['remark'] = node.xpath('..//remark')[0].text
            temp_obj['price'] = float(node.xpath('..//price')[0].text)
            temp_obj['tcPrice'] = float(node.xpath('..//tcPrice')[0].text)
            temp_obj['containItems'] = node.xpath('..//containItems')[0].text
            temp_obj['advanceDay'] = float(node.xpath('..//advanceDay')[0].text)
            temp_obj['timeLimit'] = node.xpath('..//timeLimit')[0].text
            temp_obj['updateTime'] = self.date
            ticket_list.append(temp_obj)

        # 抛出TicketDelete，删除过时的票务信息
        tickets = TicketDelete()
        tickets['id_list'] = ticket_id_list
        tickets['ly_id'] = ly_id
        yield tickets

        notice_list = []
        for notice in root.xpath('//n'):
            temp_notice = {}
            temp_notice['noticeName'] = notice.xpath('.//nTypeName')[0].text
            info_list = []
            for info in notice.xpath('.//nId'):
                temp_info = {}
                temp_info['noticeTitle'] = info.xpath('..//nName')[0].text
                temp_info['noticeContent'] = info.xpath('../nContent')[0].text
                info_list.append(temp_info)
            temp_notice['infos'] = info_list
            notice_list.append(temp_notice)

        # 抛出票务提示信息 VsTicketInfo, 通过ly_id，插入到raw_ly的ViewSpot里
        ticket_info = VsTicketInfo()
        ticket_info['info'] = notice_list
        ticket_info['ly_id'] = ly_id
        yield ticket_info

        for ticket in ticket_list:
            pid = int(ticket['policyId'])
            tk_info = copy.deepcopy(ticket)
            new_meta = {'ticket': tk_info, 'lyId': ly_id, 'pid': pid}
            request_body = {'clientIp': '127.0.0.1', 'policyId': pid, 'startDate': self.date}
            request_xml = self.assemble_req_xml(self.api['pricecalendar'], request_body)
            yield Request(
                url=self.url,
                method='POST',
                body=request_xml,
                headers={'Content-Type': 'application/x-www-form-urlencoded', 'X-Requested-With': 'XMLHttpRequest'},
                callback=self.parse_ticket_remains,
                meta=new_meta)


    def parse_ticket_remains(self, response):
        raw_xml = response.body
        ticket_info = response.meta['ticket']
        ly_id = response.meta['lyId']
        pid = response.meta['pid']

        root = etree.fromstring(raw_xml)
        stock_list = []
        for day in root.xpath('//date'):
            temp_obj = {}
            temp_obj['date'] = datetime.datetime.strptime(day.text, '%Y-%m-%d')
            left = day.xpath('..//stock')[0].text
            if left:
                temp_obj['ticketLeft'] = int(left)
            else:
                temp_obj['ticketLeft'] = None
            temp_obj['price'] = float(day.xpath('..//tcPrice')[0].text)
            stock_list.append(temp_obj)

        #抛出票务信息
        tk = Ticket()
        tk['pid'] = pid
        tk['lyId'] = ly_id
        tk['info'] = ticket_info
        tk['stock_list'] = stock_list
        yield tk


class LyViewspotPipeline(AizouPipeline):
    spiders = [LyViewspotSpider.name]
    spiders_uuid = [LyViewspotSpider.uuid]

    def process_item(self, item, spider):
        test = item
        con_ticket = get_mongodb('raw_ly', 'Ticket', profile='mongo-raw')
        con_vs = get_mongodb('raw_ly', 'ViewSpot', profile='mongo-raw')

        if item.name == 'ticket':
            con_ticket.update({'pid': item['pid']},
                              {'$set': {'pid': item['pid'], 'lyId': item['lyId'], 'info': item['info'], 'stockList': item['stock_list']}},
                               upsert=True)
        elif item.name == 'ticket-delete':
            pass
            # con_ticket.remove({'lyId': item['ly_id'], 'pid': {'$nin': item['id_list']}}, multi=True)
        elif item.name == 'vs-ticket-info':
            con_vs.update({'lyId': item['ly_id']}, {'$set': {'ticketInfo': item['info'], 'crawl': True}})

        return item