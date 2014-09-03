# coding=utf-8
import json
import re
import urlparse

import pymongo

from scrapy import Request, Selector
import scrapy
from scrapy.contrib.spiders import CrawlSpider


__author__ = 'zephyre'


class BaiduNoteItem(scrapy.Item):
    # define the fields for your item here like:
    note = scrapy.Field()


class BaiduNoteSpider(CrawlSpider):
    def __init__(self, *a, **kw):
        self.name = 'baidu_poi_image'
        super(BaiduNoteSpider, self).__init__(*a, **kw)

    def start_requests(self):
        col = pymongo.Connection().geo.Locality
        url_base = 'http://lvyou.baidu.com/search/ajax/search?format=ajax&word=%s&pn=%d'
        ret_list = list(col.find({'level': 2}, {'zhName': 1}))
        for ret in ret_list:
            url = url_base % (ret['zhName'], 0)
            yield Request(url=url, callback=self.parse_loc,
                          meta={'target': ret['zhName'], 'pn': 0, 'urlBase': url_base})
            # yield Request(url='http://chanyouji.com/users/1', callback=self.parse)

            # yield Request(url='http://lvyou.baidu.com/notes/dc6b5c3d1354b88b3b405e2e/d-0', callback=self.parse,
            # meta={'pageIdx': 0, 'noteId': 'dc6b5c3d1354b88b3b405e2e',
            # 'urlBase': 'http://lvyou.baidu.com/notes/dc6b5c3d1354b88b3b405e2e/d-'})

    def parse_loc(self, response):
        target = response.meta['target']
        pn = response.meta['pn'] + 10
        try:
            data = json.loads(response.body)

            if data['data']['search_res']['notes_list']:
                # 读取下一页
                url_base = response.meta['urlBase']
                url = url_base % (target, pn)
                yield Request(url=url, callback=self.parse_loc, meta={'target': target, 'pn': pn, 'urlBase': url_base})

            url_base = 'http://lvyou.baidu.com/notes/%s/d-%d'
            for entry in data['data']['search_res']['notes_list']:
                url = entry['loc']
                m = re.search(r'/notes/([0-9a-f]+)', url)
                if not m:
                    continue
                note_id = m.groups()[0]
                url = url_base % (note_id, 0)
                title = entry['title']
                yield Request(url=url, callback=self.parse, meta={'title': title, 'target': target, 'pageIdx': 0,
                                                                  'noteId': note_id, 'urlBase': url_base,
                                                                  'note': {'url': url, 'summary': entry}})

        except (ValueError, KeyError, TypeError):
            pass

    def parse(self, response):
        note = response.meta['note'] if 'note' in response.meta else {}
        note_id = response.meta['noteId']
        page_idx = response.meta['pageIdx']
        sel = Selector(response)

        if page_idx == 0:
            note['id'] = response.meta['noteId']
            note['target'] = response.meta['target']
            note['title'] = response.meta['title']
            # # 标题
            # ret = sel.xpath('//span[@id="J_notes-title"]/text()').extract()
            # if ret:
            # tmp = ret[0].strip()
            # if tmp:
            # note['title'] = tmp

            ret = sel.xpath('//ul[@id="J_basic-info-container"]')
            info_node = ret[0] if ret else None
            if info_node:
                ret = info_node.xpath('./li/span[contains(@class, "author-icon")]/a')
                if ret:
                    user_node = ret[0]
                    note['authorName'] = user_node.xpath('./text()').extract()[0]
                    tmp = user_node.xpath('./@href').extract()[0]
                    m = re.compile(r'[^/]+$').search(tmp)
                    if m:
                        note['authorId'] = m.group()

                ret = info_node.xpath('./li//span[contains(@class, "start_time")]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    if tmp:
                        note['startTime'] = tmp

                ret = info_node.xpath('./li/span[contains(@class, "path-icon")]/span[@class="infos"]/text()').extract()
                if ret and len(ret) == 2:
                    tmp = ret[0].strip()
                    m = re.search(ur'^从(.+)', tmp)
                    if m:
                        note['fromLoc'] = m.groups()[0]
                    tmp = ret[1].strip()
                    if tmp:
                        note['toLoc'] = filter(lambda val: val and not re.match(ur'^\s*\.+\s*$', val),
                                               list(tmp.strip() for tmp in tmp.split(u'、')))
                ret = info_node.xpath('./li/span[contains(@class, "time-icon")]/span[@class="infos"]/text()').extract()
                if ret:
                    m = re.search(ur'(\d+)天', ret[0].strip())
                    if m:
                        note['timeCost'] = int(m.groups()[0])

                ret = info_node.xpath('./li/span[contains(@class, "cost-icon")]/span[@class="infos"]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    if tmp:
                        note['cost'] = tmp

                ret = info_node.xpath('./li[contains(@class, "notes-info-foot")]/div[@class="fl"]/text()').extract()
                if ret:
                    tmp = ret[0].strip()
                    m = re.search(ur'回\s*复\s*(\d+)', tmp)
                    if m:
                        note['replyCnt'] = int(m.groups()[0])
                    m = re.search(ur'浏\s*览\s*(\d+)', tmp)
                    if m:
                        note['viewCnt'] = int(m.groups()[0])

        if 'contents' not in note:
            note['contents'] = []
        contents = note['contents']
        contents_list = sel.xpath(
            '//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]/div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]').extract()
        if contents_list:
            contents.extend(contents_list)

        # 是否存在下一页？
        tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
        if not tmp or not contents_list:
            url_t = 'http://lvyou.baidu.com/notes/%s-%d'
            url = url_t % (note['id'], len(note['contents']))
            yield Request(url=url, callback=self.parse_comments, meta={'urlT': url_t, 'note': note})
        else:
            page_idx += 1
            url_base = response.meta['urlBase']
            url = url_base % (note_id, page_idx)
            yield Request(url=url, callback=self.parse,
                          meta={'pageIdx': page_idx, 'noteId': note_id, 'urlBase': url_base, 'note': note})

    def parse_comments(self, response):
        note = response.meta['note']

        if 'comments' not in note:
            note['comments'] = []
        comments = note['comments']
        author = note['authorName']

        sel = Selector(response)

        node_list = sel.xpath('//div[@id="building-container"]//div[contains(@class, "grid-s5m0")]')
        for node in node_list:
            ret = node.xpath('./div[@class="col-main"]/div[@class="floor"]/div[@class="floor-content"]')
            if not ret:
                continue
            c_node = ret[0]
            ret = c_node.xpath('./@nickname').extract()
            if not ret or (ret[0] == author and not comments):
                continue
            c_author = ret[0]
            ret = c_node.xpath('./@uid').extract()
            if not ret:
                continue
            c_author_id = ret[0]

            tmp = c_node.extract()
            if tmp:
                comments.append({'authorName': c_author, 'authorId': c_author_id, 'comment': tmp})

        # 检查是否有下一页
        tmp = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/text()').extract()
        tmp = tmp[-1] if tmp else None
        if tmp:
            try:
                tmp = int(tmp)
            except ValueError:
                tmp = None

        if not tmp:
            tmp_href = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@class="nslog"]/@href').extract()
            if tmp_href:
                href = tmp_href[-1]
                parts = urlparse.urlparse(response.url)
                url = urlparse.urlunparse((parts[0], parts[1], href, '', '', ''))
                return Request(url=url, callback=self.parse_comments,
                               meta={'urlT': response.meta['urlT'], 'note': note})

        item = BaiduNoteItem()
        item['note'] = note
        return item


class BaiduNotePipeline(object):
    def process_item(self, item, spider):
        if not isinstance(item, BaiduNoteItem):
            return item

        col = pymongo.Connection().raw_notes.BaiduNote
        ret = col.find_one({'id': item['note']['id']}, {'_id': 1})
        if ret:
            item['note']['_id'] = ret['_id']

        col.save(item['note'])

