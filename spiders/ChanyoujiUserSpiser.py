# coding=utf-8
import json

import pymongo


__author__ = 'wdx'
# coding=utf-8

import re

from scrapy import Request, Selector
from scrapy.contrib.spiders import CrawlSpider

from items import ChanyoujiUser


class ChanyoujiUserSpider(CrawlSpider):
    name = 'chanyouji_user'

    allowed_domains = ["chanyouji.com"]

    def start_requests(self):
        url_template = 'http://chanyouji.com/users/%d'

        lower = 1
        upper = 400000
        if 'param' in dir(self):
            param = getattr(self, 'param')
            if 'lower' in param:
                lower = int(param['lower'][0])
            if 'upper' in param:
                upper = int(param['upper'][0])

        for user_id in range(lower, upper):
            url = url_template % user_id
            m = {'user_id': user_id}
            yield Request(url=url, callback=self.parse, meta={'data': m})

    def parse(self, response):
        sel = Selector(response)
        item = ChanyoujiUser()
        user_data = response.meta['data']
        item['user_id'] = user_data['user_id']
        user_name = sel.xpath('//div[contains(@class, "header-inner")]/h1/text()').extract()
        if user_name:
            item['user_name'] = user_name[0]
        else:
            item['user_name'] = None
        ret = sel.xpath('//div[contains(@class, "header-inner")]/div[1]/text()').extract()
        if ret:
            num_youji = ret[0]
            num = re.compile('\d{1,}')
            m1 = num.search(num_youji)
            if m1:
                item['num_notes'] = int(m1.group())

        ret = sel.xpath(
            '//div[contains(@class,"header-inner")]/a/img[contains(@class,"avatar") and @src]/@src').extract()
        if ret:
            item['avatar'] = ret[0]

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"weibo")]/a/@href').extract()
        if ret:
            weibo_url = ret[0]
            item['weibo_url'] = weibo_url

            match = re.search(r'weibo\.com/u/(\d+)/?$', weibo_url)
            if match:
                item['weibo_uid'] = int(match.groups()[0])
            else:
                match = re.search(r'weibo\.com/([^/]+)/?$', weibo_url)
                if match:
                    item['weibo_uid'] = match.groups()[0]

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"douban")]/a/@href').extract()
        if ret:
            douban_url = ret[0]
            item['douban_url'] = douban_url
            match = re.search(r'douban\.com/people/(\d+)/?$', douban_url)
            if match:
                item['douban_uid'] = int(match.groups()[0])

        ret = sel.xpath(
            '//div[contains(@class, "sns-site")]/ul[@class="sns-ico"]/li[contains(@class,"renren")]/a/@href').extract()
        if ret:
            renren_url = ret[0]
            item['renren_url'] = renren_url
            match = re.search(r'renren\.com/(\d+)/profile/?$', renren_url)
            if match:
                item['renren_uid'] = int(match.groups()[0])

        marker = {}
        # 查找Gmaps.map.markers对象
        match = re.search(r'Gmaps\.map\.markers\s*=\s*(?=\[)(.+?)(?<=\])', response.body)
        if match:
            try:
                marker_data = json.loads(match.groups()[0])
                for tmp in marker_data:
                    lat = float(tmp['lat'])
                    lng = float(tmp['lng'])
                    mid = tmp['id']
                    title = tmp['title'].strip()
                    desc = tmp['description']

                    match = re.search(r'href\s*="([^"]+)"', desc)
                    href = 'http://chanyouji.com' + match.groups()[0] if match else None
                    if href:
                        marker[mid] = {'lat': lat, 'lng': lng, 'title': title, 'url': href, 'data_id': mid}
            except (ValueError, KeyError):
                pass

        traveled_list = []
        for data_id in sel.xpath(
                '//ul[@id="attraction_markers_list"]//a[contains(@class,"node") and @data-id]/@data-id').extract():
            data_id = int(data_id)
            if data_id not in marker:
                continue
            traveled_list.append(marker[data_id])

        item['traveled'] = traveled_list

        if not item['traveled']:
            yield item
        else:
            yield Request(url=item['traveled'][0]['url'], callback=self.parse_note, meta={'item': item})

    def parse_note(self, response):
        item = response.meta['item']

        match = re.search(
            r'_G_trip_collection\s*=\s*new\s*tripshow\.TripsCollection\((?=\[)(.+?),\s*\{\s*parse\s*:\s*(true|false)',
            response.body)
        if not match:
            item['traveled'] = []
            return item

        vs_map = {}
        data = filter(lambda val: 'entry' in val and 'attraction_id' in val['entry'], json.loads(match.groups()[0]))
        for tmp in data:
            tmp = tmp['entry']
            vs_map[tmp['name_zh_cn']] = tmp

        traveled = item['traveled']
        candidate = filter(lambda val: 'data_id' in val, traveled)
        traveled = filter(lambda val: 'data_id' not in val, traveled)

        next_url = None
        for tmp in candidate:
            title = tmp['title']
            if title in vs_map:
                data = vs_map[title]
                data['lat'] = tmp['lat']
                data['lng'] = tmp['lng']
                traveled.append(data)
            else:
                traveled.append(tmp)
                next_url = tmp['url']

        item['traveled'] = traveled
        if next_url:
            return Request(url=next_url, callback=self.parse_note, meta={'item': item})
        else:
            return item


class ChanyoujiUserPipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, ChanyoujiUser):
            return item

        col = pymongo.MongoClient().raw_data.ChanyoujiUser

        uid = item['user_id']
        data = {'userId': uid,
                'userName': item['user_name']}

        if 'avatar' in item:
            data['avatar'] = item['avatar']
        if 'weibo_url' in item:
            data['weiboUrl'] = item['weibo_url']
        if 'weibo_uid' in item:
            data['weiboUid'] = item['weibo_uid']
        if 'renren_url' in item:
            data['renrenUrl'] = item['renren_url']
        if 'renren_uid' in item:
            data['renrenUid'] = item['renren_uid']
        if 'douban_url' in item:
            data['doubanUrl'] = item['douban_url']
        if 'douban_uid' in item:
            data['doubanUid'] = item['douban_uid']
        if 'traveled' in item:
            data['traveled'] = item['traveled']
        if 'num_notes' in item:
            data['numNotes'] = item['num_notes']

        ret = col.find_one({'userId': uid})
        if not ret:
            ret = {}
        for k in data:
            ret[k] = data[k]
        col.save(ret)

        return item









