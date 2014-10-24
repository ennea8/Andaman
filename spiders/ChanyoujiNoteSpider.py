# -*- coding: UTF-8 -*-
import conf

__author__ = 'wdx'

import json
import re
import utils
import pysolr
import time
import datetime

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiYoujiItem, ChanyoujiNoteProcItem

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class ChanyoujiYoujiSpider(CrawlSpider):
    name = "chanyouji_note"

    def start_requests(self):
        template_url = "http://chanyouji.com/trips/%d"

        lower = 1
        upper = 400000
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])

        for trips_id in range(lower, upper):
            url = template_url % trips_id
            m = {'trips_id': trips_id, 'url': url}
            yield Request(url=url, callback=self.parse, meta={"data": m})

    def parse(self, response):
        sel = Selector(response)
        item = ChanyoujiYoujiItem()
        item['trips_id'] = response.meta['data']['trips_id']

        match_title = re.search(r'_G_trip_name="[\s\S][^"]*', response.body)
        if not match_title:
            return
        item['title'] = match_title.group()[14:]

        authorName = sel.xpath('//div[@class="trip-info"]//a/text()').extract()
        if authorName:
            item['authorName'] = authorName[0]

        favorCnt = sel.xpath('//div[@class="counter"]/span[@class="favorites-num"]/span/text()').extract()
        if favorCnt:
            item['favorCnt'] = favorCnt[0]

        commentCnt = sel.xpath('//div[@class="counter"]/span[@class="comments-num"]/span/text()').extract()
        if commentCnt:
            item['commentCnt'] = commentCnt[0]

        viewCnt = sel.xpath('//div[@class="counter"]/span[@class="viewer-num"]/span/text()').extract()
        if viewCnt:
            item['viewCnt'] = viewCnt[0]

        match = re.search(
            r'_G_trip_collection\s*=\s*new\s*tripshow\.TripsCollection\((?=\[)(.+?),\s*\{\s*parse\s*:\s*(true|false)',
            response.body)
        if not match:
            return
        try:
            item['data'] = json.loads(match.groups()[0])
        except ValueError:
            return

        authorId_m = sel.xpath('//div[@class="trip-info"]/a/@href').extract()

        item['authorAvatar'] = None
        if authorId_m:
            authorId = authorId_m[0]
            item['authorId'] = authorId[7:]
            author_url = "http://chanyouji.com/users/%d" % int(item['authorId'])
            yield Request(url=author_url, callback=self.parse_next, meta={"data": item})
        else:
            yield item


    def parse_next(self, response):
        sel = Selector(response)
        item = ChanyoujiYoujiItem()
        item = response.meta['data']

        authorAvatar = sel.xpath('//div[contains(@class,"header-inner")]//img/@src').extract()
        if authorAvatar:
            item['authorAvatar'] = authorAvatar[0]

        yield item


class ChanyoujiYoujiPipline(object):
    spiders = [ChanyoujiYoujiSpider.name]

    def process_item(self, item, spider):
        if not isinstance(item, ChanyoujiYoujiItem):
            return item

        col = utils.get_mongodb('raw_data', 'ChanyoujiNote1', profile='mongodb-crawler')
        note = {'noteId': item['trips_id'],
                'title': item['title'],
                'authorName': item['authorName'],
                'favorCnt': item['favorCnt'],
                'commentCnt': item['commentCnt'],
                'viewCnt': item['viewCnt'],
                'note': item['data'],
                'authorAvatar': item['authorAvatar'],
                'authorId': item['authorId']
        }
        ret = col.find_one({'noteId': note['noteId']})
        if not ret:
            ret = {}
        for k in note:
            ret[k] = note[k]
        col.save(ret)

        return item


class ChanyoujiNoteProcSpider(CrawlSpider):
    name = 'chanyouji_note_proc'  # name of spider

    def __init__(self, *a, **kw):
        super(ChanyoujiNoteProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        yield Request(url='http://www.baidu.com', callback=self.parse)

    def parse(self, response):
        item = ChanyoujiNoteProcItem()
        col = utils.get_mongodb('raw_data', 'ChanyoujiNote1', profile='mongodb-crawler')
        part = col.find()
        content_m=[]
        for entry in part:
            contents = []
            toLoc = []
            date_num = []
            day_num = []
            content_m = entry['note']
            note_len = len(content_m)
            for i in range(note_len):
                if 'trip_date' in content_m[i]:
                    date_num.append(content_m[i]['trip_date'])

                if 'day' in content_m[i]:
                    day_num.append(content_m[i]['day'])
                    day_text = '第%d天' % content_m[i]['day']
                    if day_text not in contents:
                        contents.append(day_text)

                if 'entry' in content_m[i]:
                    if 'name_zh_cn' in content_m[i]['entry']:
                        toLoc.append(content_m[i]['entry']['name_zh_cn'])

                if 'description' in content_m[i]:
                    if  content_m[i]['description']:
                        contents.append(content_m[i]['description'])

                if 'photo' in content_m[i]:
                    if 'src' in content_m[i]['photo']:
                        contents.append(content_m[i]['photo']['src'])


            item['id'] = entry['_id']
            item['title'] = entry['title']
            item['authorName'] = entry['authorName']
            item['authorAvatar'] = entry['authorAvatar']
            item['publishDate'] = None
            item['favorCnt'] = int(entry['favorCnt'])
            item['commentCnt'] = int(entry['commentCnt'])
            item['viewCnt'] = int(entry['viewCnt'])
            item['costLower'] = None
            item['costUpper'] = None
            item['costNorm'] = None
            item['source'] = 'chanyouji'
            item['sourceUrl'] = 'http://chanyouji/trips/%d' % entry['noteId']
            item['startDate'] = None
            item['endDate'] = None
            item['elite'] = None
            item['days'] = int(day_num[-1])
            item['summary'] = None
            item['contents'] = contents
            item['toLoc'] = toLoc
            item['fromLoc'] = None
            if (item['viewCnt']>1500) and (len(item['contents'])>150) or item['favorCnt']>100:
                item['elite'] = True
            else :
                item['elite'] = False
            if date_num[0]:
                date0=date_num[0].replace("-0","-")
                startDate_v = re.split('[-]', date0)
                item['startDate'] = datetime.datetime(int(startDate_v[0]), int(startDate_v[1]), int(startDate_v[2]))
            if date_num[-1]:
                date1=date_num[-1].replace("-0","-")
                endDate_v = re.split('[-]', date1)
                item['endDate'] = datetime.datetime(int(endDate_v[0]), int(endDate_v[1]), int(endDate_v[2]))
            yield item






            # items.append(item)



class ChanyoujiNoteProcPipeline(object):
    """
    上传禅游记信息
    """

    spiders = [ChanyoujiNoteProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != ChanyoujiNoteProcItem.__name__:
            return item

        solr_conf = conf.global_conf['solr']
        solr_s = pysolr.Solr('http://%s:%s/solr' % (solr_conf['host'], solr_conf['port']))
        doc = [{'id': str(item['id']),
                'title': item['title'],
                'authorName': item['authorName'],
                'authorAvatar': item['authorAvatar'],
                'publishDate': item['publishDate'],
                'favorCnt': item['favorCnt'],
                'commentCnt': item['commentCnt'],
                'viewCnt': item['viewCnt'],
                'costLower': item['costLower'],
                'costUpper': item['costUpper'],
                'costNorm': item['costNorm'],
                'days': item['days'],
                'fromLoc': item['fromLoc'],
                'toLoc': item['toLoc'],
                'summary': item['summary'],
                'contents': item['contents'],
                'startDate': item['startDate'],
                'endDate': item['endDate'],
                'source': item['source'],
                'sourceUrl': item['sourceUrl'],
                'elite': item['elite']
               }]

        solr_s.add(doc)

        return item










