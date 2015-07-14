# coding=utf-8
from itertools import imap
import json

from scrapy import log
import scrapy

import re
from andaman.items.baidu import BaiduNoteItem

__author__ = 'zephyre'

# 用一个全局的dict来存储301重定向的href和surl一一对应的关系
global PLACE_DICT
PLACE_DICT = {}

def image_builder(key):
    return 'http://hiphotos.baidu.com/lvpics/pic/item/%s.jpg' % key


class BaiduNoteSpider(scrapy.Spider):
    name = 'baidu-note'

    def __init__(self, **kwargs):
        super(BaiduNoteSpider, self).__init__(**kwargs)
        self.note_offset = int(getattr(self, 'note_offset', 0))
        self.note_limit = int(getattr(self, 'note_limit', 2))
        self.note_type = int(getattr(self, 'note_type', 3))

    def start_requests(self):
        step_size = 20
        self.note_limit = (self.note_limit / step_size + 1) * step_size
        self.log('Spider started. Note type: %d, offset: %d, count: %d'
                 % (self.note_type, self.note_offset, self.note_limit), level=log.INFO)
        return imap(lambda pn: scrapy.Request(
            'http://lvyou.baidu.com/search/ajax/searchnotes?format=ajax&type=3&pn=%d&rn=20' % pn),
                    xrange(self.note_offset, self.note_limit, step_size))

    def parse(self, response):
        """
        @url http://lvyou.baidu.com/search/ajax/searchnotes?format=ajax&type=3&pn=0&rn=20
        @returns items 20 20
        @returns requests 0 0
        @scrapes title author_name author_avatar abstract view_cnt vote_cnt comment_cnt favor_cnt
        """

        def build_note(entry):
            """
            从response中的一段数据，生成一个Note，以及访问游记详情的Request

            :param entry:
            :return:
            """

            one_note = {}
            one_note['note_id'] = entry['nid']
            one_note['title'] = entry['title'].strip()
            one_note['author_name'] = entry['user_nickname'].strip()
            one_note['author_avatar'] = image_builder(entry['avatar_small'].strip())
            one_note['abstract'] = entry['content'].strip()
            one_note['view_cnt'] = int(entry['view_count'])
            one_note['vote_cnt'] = int(entry['recommend_count'])
            one_note['comment_cnt'] = int(entry['common_posts_count'])
            one_note['favor_cnt'] = int(entry['favorite_count'])
            one_note['post_count'] = int(entry['notes_posts_count'])
            one_note['raw_data'] = entry
            one_note['departure'] = entry['departure']
            try:
                destinations = [x['surl'] for x in entry['destinations']]
            except TypeError:
                destinations = [x for x in entry['destinations']]
            one_note['destinations'] = destinations
            one_note['durationtime'] = int(entry['time'])
            one_note['durationtime_unit'] = entry['time_unit']
            one_note['start_month'] = int(entry['start_month'])
            one_note['publish_time'] = long(entry['publish_time']) * 1000L
            one_note['brief_album'] = map(image_builder, [x['pic_url'] for x in entry['album_pic_list']])

            # 生成正文的请求,下一步才是解析获得的页面，包括PostItem和path
            req = scrapy.Request(url='http://lvyou.baidu.com/notes/%s/d-0' % one_note['note_id'],
                                 callback=self.parse_detail,
                                 meta={'one_note': one_note, 'max_page': 0, 'current_page': 0,
                                       'place_href': []})

            return req

        # 游记的综合信息
        # 用来做测试
        note_id = [x['nid'] for x in json.loads(response.body_as_unicode())['data']['notes_list']]
        PLACE_DICT['note_id'] = note_id
        return imap(build_note, json.loads(response.body_as_unicode())['data']['notes_list'])

    # 处理楼层分页的信息,
    # 多想想几种情况 ，测试用例来测试一下
    def parse_detail(self, response):
        meta = response.meta

        one_note = meta['one_note']
        note_id = one_note['note_id']

        current_page = meta['current_page']
        max_page = meta['max_page']

        place_href = meta['place_href']

        sel = scrapy.Selector(response)
        # 由meta中的page来判定是否是第一页
        if current_page == 0:
            # 若是第一页则创建one_noter['contents']
            one_note['contents'] = []

            # 若是第一页,则解析出其中的最大页数，更新max_page
            # 若不存在，则默认max_page为0
            href_list = sel.xpath('//span[@id="J_notes-view-pagelist"]/a[@href]/@href').extract()
            max_page = max(map(lambda x: int(re.search('d-(\d+)', x).group(1)),
                               href_list)) if href_list else 0

            # 首页若存在place则解析path,不存在则place_href为空
            # place_href只用meta作传递用进行传递，直到最后一页时进行地点的处理
            place_href = sel.xpath('//div[@class="detail-floor1-path"]//span[@class="path-detail"]/a/@href').extract()
            # 当存在时，one_note里要包含path这一项
            # 若不存在，则在处理地点的函数中，直接生成item
            if place_href:
                one_note['path'] = []

        # 由xpath选出每页中的所有的游记内容
        post_list = sel.xpath('//div[@class="col-main"]/div')
        for node in post_list:
            # 每一楼都要有floor_id and contents
            floor_id = node.xpath('./@id').extract()[0]
            floor_contents = node.xpath('.//div[@class="floor-content"]').extract()[0]
            per_floor = {'floor_id': floor_id, 'floor_contents': floor_contents}
            one_note['contents'].append(per_floor)

        # 由maxpage生成要访问的翻页,从第二页开始
        # current_page == max_page则要处理meta中的place
        if current_page == max_page:
            # 当是第一页，且没有place需要解析时，可直接生成item
            if max_page == 0:
                if not place_href:
                    for item in create_item(one_note):
                        yield item
                elif place_href:
                    for place in self.parse_place(one_note, place_href):
                        yield place

            # 当到达最后一页时，处理place，在处理place的函数中生成item
            # one_note即是要生成的item，meta含要处理的数据
            else:
                for place in self.parse_place(one_note, meta['place_href']):
                    yield place
        else:
            current_page += 1
            yield scrapy.Request(url='http://lvyou.baidu.com/notes/%s/d-%d' % (note_id, current_page),
                                 callback=self.parse_detail,
                                 meta={'one_note': one_note, 'max_page': max_page, 'current_page': current_page,
                                       'place_href': place_href})
    # 输入输出要重新定
    # 关于place的所有处理都包含在这个函数里
    # 用来处理首次的place处理，后续的处理包含在parse_href中
    def parse_place(self, one_note, place_href):
        # 若没有place需要处理，则生成item
        if not place_href:
            for item in create_item(one_note):
                yield item

        # 对plae_href去重
        place_href_uniq = list(set(place_href))
        # 分为直接存在surl和301重定向处理两部分处理
        # re_surl 专门用来存储重定向的href
        re_surl = []
        for x in place_href_uniq:
            match = re.match(r'/scene/', x)
            # 没有成功匹配，则说明x是surl, 直接加入到path中即可
            if not match:
                path_surl = x.strip('/')
                one_note['path'].append(path_surl)
            else:
                # 匹配成功，则说明x是重定向网址, 另行处理,存入另一个列表中
                re_surl.append(x)
        # 如果有重定向的href存在
        if not re_surl:
            # 重定向的地址为0，则说明不需要处理地址了，直接生成item
            for item in create_item(one_note):
                yield item
        else:
            # 再用一个函数单独处理redirct surl
            y = re_surl.pop()
            if y in PLACE_DICT.keys():
                one_note['path'].append(PLACE_DICT[y])
                if not re_surl:
                    create_item(one_note)
            else:
                tmp_url = 'http://lvyou.baidu.com%s' % y
                yield scrapy.Request(url=tmp_url, callback=self.parse_href,
                                     meta={'dont_redirect': True, 'handle_httpstatus_list': [301, 302],
                                           'one_note': one_note, 're_surl': re_surl, 'key': y})

    def parse_href(self, response):
        meta = response.meta

        one_note = meta['one_note']
        re_surl = meta['re_surl']
        key = meta['key']

        # 首先要处理一条信息，加入one_note的path中
        href_surl = response.headers['location'].strip('/')
        one_note['path'].append(href_surl)
        # 得到href_surl后，要加入到PLACE_DICT中
        PLACE_DICT[key] = href_surl

        # 判断re_surl中是否还有元素，若没有则可生成item
        if not re_surl:
            for item in create_item(one_note):
                yield item
        # 若还有元素，则继续进行处理
        else:
            x = re_surl.pop()
            if x in PLACE_DICT.keys():
                one_note['path'].append(PLACE_DICT[x])
                if not re_surl:
                    for item in create_item(one_note):
                        yield item
            else:
                tmp_url = 'http://lvyou.baidu.com%s' % x
                yield scrapy.Request(url=tmp_url, callback=self.parse_href,
                                     meta={'dont_redirect': True, 'handle_httpstatus_list': [301, 302],
                                           'one_note': one_note, 're_surl': re_surl, 'key': x})

def create_item(one_note):
    item = BaiduNoteItem()
    item['one_note'] = one_note

    yield item

